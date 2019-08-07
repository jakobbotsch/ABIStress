using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Reflection.Metadata;
using System.Runtime.InteropServices;
using System.Runtime.Loader;

namespace ABIStress
{
    internal class Program
    {
        private static int Main(string[] args)
        {
            void Usage()
            {
                Console.WriteLine("Usage: [--caller-index <number>] [--num-calls <number>] [--verbose]");
                Console.WriteLine("Either --caller-index or --num-calls must be specified.");
            }

            if (args.Contains("-help") || args.Contains("--help") || args.Contains("-h"))
            {
                Usage();
                return 1;
            }

            Config.Verbose = args.Contains("--verbose");
            int callerIndex = -1;
            int numCalls = -1;
            int argIndex;
            if ((argIndex = Array.IndexOf(args, "--caller-index")) != -1)
                callerIndex = int.Parse(args[argIndex + 1]);
            if ((argIndex = Array.IndexOf(args, "--num-calls")) != -1)
                numCalls = int.Parse(args[argIndex + 1]);

            if ((callerIndex == -1) == (numCalls == -1))
            {
                Usage();
                return 1;
            }

            List<Callee> callees = CreateCallees(Config.NumCallees);

            using var tcel = new TailCallEventListener();

            int mismatches = 0;
            if (callerIndex != -1)
            {
                if (!DoCall(callerIndex, callees))
                    mismatches++;
            }
            else
            {
                bool abortLoop = false;
                Console.CancelKeyPress += (sender, args) =>
                {
                    args.Cancel = true;
                    abortLoop = true;
                };

                for (int i = 0; i < numCalls && !abortLoop; i++)
                {
                    if (!DoCall(i, callees))
                        mismatches++;

                    if (i % 50 == 0)
                        Console.WriteLine($"{tcel.NumCallersSeen} callers emitted, {tcel.NumSuccessfulTailCalls} tailcalls tested");
                }
            }

            Console.WriteLine("{0} tailcalls tested", tcel.NumSuccessfulTailCalls);
            lock (tcel.FailureReasons)
            {
                if (tcel.FailureReasons.Count != 0)
                {
                    int numRejected = tcel.FailureReasons.Values.Sum();
                    Console.WriteLine("{0} rejected tailcalls. Breakdown:", numRejected);
                    foreach (var (reason, count) in tcel.FailureReasons.OrderByDescending(kvp => kvp.Value))
                        Console.WriteLine("[{0:00.00}%]: {1}", count / (double)numRejected * 100, reason);
                }
            }

            return 100 + mismatches;
        }

        private static List<Callee> CreateCallees(int count)
        {
            List<Callee> callees = Enumerable.Range(0, count).Select(CreateCallee).ToList();
            return callees;
        }

        private static Callee CreateCallee(int calleeIndex)
        {
            string name = Config.CalleePrefix + calleeIndex;
            Random rand = new Random(Config.Seed - calleeIndex);
            List<TypeEx> pms = RandomParameters(rand);
            var tc = new Callee(name, pms);
            return tc;
        }

        private static List<TypeEx> RandomParameters(Random rand)
        {
            List<TypeEx> pms = new List<TypeEx>(rand.Next(1, 7));
            for (int j = 0; j < pms.Capacity; j++)
                pms.Add(s_candidateArgTypes[rand.Next(s_candidateArgTypes.Length)]);

            return pms;
        }

        private static readonly List<DynamicMethod> s_keepRooted = new List<DynamicMethod>();
        private static bool DoCall(int callerIndex, List<Callee> callees)
        {
            string callerName = Config.CallerPrefix + callerIndex;

            // Use a known starting seed so we can test a single caller easily.
            Random rand = new Random(Config.Seed + callerIndex);
            List<TypeEx> pms = RandomParameters(rand);
            // Get candidate callees. It is a hard requirement that the caller has more stack space.
            int argStackSizeApprox = s_abi.ApproximateArgStackAreaSize(pms);
            List<Callee> callable = callees.Where(t => t.ArgStackSizeApprox < argStackSizeApprox).ToList();
            if (callable.Count <= 0)
                return true;

            int calleeIndex = rand.Next(callable.Count);
            // We might not have emitted this callee yet, so do that if so.
            if (callable[calleeIndex].Method == null)
            {
                callable[calleeIndex].Emit();
                Debug.Assert(callable[calleeIndex].Method != null);
            }

            Callee callee = callable[calleeIndex];

            void DumpCallerToCalleeInfo()
            {
                Console.WriteLine("{0} -> {1}", callerName, callee.Name);
                Console.WriteLine("Caller signature: {0}", string.Join(", ", pms.Select(pm => pm.Type.Name)));
                Console.WriteLine("Callee signature: {0}", string.Join(", ", callee.Parameters.Select(pm => pm.Type.Name)));
            }

            if (Config.Verbose)
                DumpCallerToCalleeInfo();

            // Now create the args to pass to the callee from the caller.
            List<Value> args = new List<Value>(callee.Parameters.Count);
            List<Value> candidates = new List<Value>();
            for (int j = 0; j < args.Capacity; j++)
            {
                TypeEx targetTy = callee.Parameters[j];
                // Collect candidate args. For each parameter to the caller we might be able to just
                // forward it or one of its fields.
                candidates.Clear();
                CollectCandidateArgs(targetTy.Type, pms, candidates);

                if (candidates.Count > 0)
                {
                    args.Add(candidates[rand.Next(candidates.Count)]);
                }
                else
                {
                    // No candidates to forward, so just create a new value here dynamically.
                    args.Add(new ConstantValue(targetTy, Gen.GenConstant(targetTy.Type, targetTy.Fields, rand)));
                }
            }

            // We test both calls through calli and a tailcall. To record the results of calli, we pass a
            // int[] as the last arg where the caller will record the results.
            Type[] finalParams = pms.Select(t => t.Type).Concat(new[] { typeof(int[]) }).ToArray();
            DynamicMethod caller = new DynamicMethod(
                callerName, typeof(int), finalParams, typeof(Program).Module);

            // We need to keep callers rooted due to a stale cache bug in the runtime.
            s_keepRooted.Add(caller);

            ILGenerator g = caller.GetILGenerator();
            if (Config.Verbose)
            {
                EmitDumpArgList(g, pms, j => g.Emit(OpCodes.Ldarg, checked((short)j)), "Caller incoming args");
            }

            // Emit pinvoke calls for each calling convention. Keep delegates rooted.
            LocalBuilder resultLocal = g.DeclareLocal(typeof(int));
            List<Delegate> delegates = new List<Delegate>();
            int resultIndex = 0;
            foreach (var (cc, delegateType) in callee.DelegateTypes)
            {
                Delegate dlg = callee.Method.CreateDelegate(delegateType);
                delegates.Add(dlg);

                if (Config.Verbose)
                    EmitDumpArgList(g, args.Select(a => a.Type), j => args[j].Emit(g), $"Caller passing args to {cc} calli");

                for (int j = 0; j < args.Count; j++)
                    args[j].Emit(g);

                IntPtr ptr = Marshal.GetFunctionPointerForDelegate(dlg);
                g.Emit(OpCodes.Ldc_I8, (long)ptr);
                g.Emit(OpCodes.Conv_I);
                g.EmitCalli(OpCodes.Calli, cc, typeof(int), callee.Parameters.Select(p => p.Type).ToArray());
                g.Emit(OpCodes.Stloc, resultLocal);

                g.Emit(OpCodes.Ldarg, (short)(finalParams.Length - 1)); // reference to int[]
                g.Emit(OpCodes.Ldc_I4, resultIndex); // where to store result
                g.Emit(OpCodes.Ldloc, resultLocal); // result
                g.Emit(OpCodes.Stelem_I4);
                resultIndex++;
            }

            // Finally do tailcall.
            if (Config.Verbose)
                EmitDumpArgList(g, args.Select(a => a.Type), j => args[j].Emit(g), "Caller passing args to tailcall");

            for (int j = 0; j < args.Count; j++)
                args[j].Emit(g);

            g.Emit(OpCodes.Tailcall);
            g.EmitCall(OpCodes.Call, callee.Method, null);
            g.Emit(OpCodes.Ret);

            object[] outerArgs = pms.Select(t => Gen.GenConstant(t.Type, t.Fields, rand)).ToArray();
            object[] innerArgs = args.Select(v => v.Get(outerArgs)).ToArray();

            if (Config.Verbose)
            {
                Console.WriteLine("Invoking caller through reflection with args");
                for (int j = 0; j < outerArgs.Length; j++)
                {
                    Console.Write($"arg{j}=");
                    DumpObject(outerArgs[j]);
                }
            }
            int[] pinvokeResult = new int[callee.DelegateTypes.Count];
            object result = InvokeMethodDynamicallyButWithoutReflection(caller, outerArgs, pinvokeResult);
            //object result = caller.Invoke(null, outerArgs);

            if (Config.Verbose)
            {
                Console.WriteLine("Invoking callee through reflection with args");
                for (int j = 0; j < innerArgs.Length; j++)
                {
                    Console.Write($"arg{j}=");
                    DumpObject(innerArgs[j]);
                }
            }
            //object expectedResult = callee.Method.Invoke(null, innerArgs);
            object expectedResult = InvokeMethodDynamicallyButWithoutReflection(callee.Method, innerArgs, null);

            GC.KeepAlive(delegates);

            bool allCorrect = true;
            for (int i = 0; i < pinvokeResult.Length + 1; i++)
            {
                int thisResult = i == 0 ? (int)result : pinvokeResult[i - 1];
                if (thisResult == (int)expectedResult)
                    continue;

                allCorrect = false;
                string callType = i == 0 ? "Tailcall" : callee.DelegateTypes.ElementAt(i - 1).Key.ToString();
                Console.WriteLine("Mismatch in {0}: {1} ({2} params) -> {3} ({4} params) (expected {5}, got {6})",
                    callType,
                    callerName, pms.Count,
                    callee.Name, callee.Parameters.Count,
                    expectedResult, thisResult);
            }

            if (!allCorrect)
                DumpCallerToCalleeInfo();

            return allCorrect;
        }

        // This function works around a reflection bug on ARM64:
        // https://github.com/dotnet/coreclr/issues/25993
        private static object InvokeMethodDynamicallyButWithoutReflection(MethodInfo mi, object[] args, int[] recordResult)
        {
            DynamicMethod dynCaller = new DynamicMethod(
                $"DynCaller", typeof(object), new Type[] { typeof(int[]) }, typeof(Program).Module);

            ILGenerator g = dynCaller.GetILGenerator();
            foreach (var arg in args)
                new ConstantValue(new TypeEx(arg.GetType()), arg).Emit(g);

            if (recordResult != null)
                g.Emit(OpCodes.Ldarg_0);

            g.Emit(OpCodes.Call, mi);
            g.Emit(OpCodes.Box, mi.ReturnType);
            g.Emit(OpCodes.Ret);
            Func<int[], object> f = (Func<int[], object>)dynCaller.CreateDelegate(typeof(Func<int[], object>));
            return f(recordResult);
        }

        private static void CollectCandidateArgs(Type targetTy, List<TypeEx> pms, List<Value> candidates)
        {
            for (int i = 0; i < pms.Count; i++)
            {
                TypeEx pm = pms[i];
                Value arg = null;
                if (pm.Type == targetTy)
                    candidates.Add(arg = new ArgValue(pm, i));

                if (pm.Fields == null)
                    continue;

                for (int j = 0; j < pm.Fields.Length; j++)
                {
                    FieldInfo fi = pm.Fields[j];
                    if (fi.FieldType != targetTy)
                        continue;

                    arg ??= new ArgValue(pm, i);
                    candidates.Add(new FieldValue(arg, j));
                }
            }
        }

        private static readonly MethodInfo s_writeString = typeof(Console).GetMethod("Write", new[] { typeof(string) });
        private static readonly MethodInfo s_writeLineString = typeof(Console).GetMethod("WriteLine", new[] { typeof(string) });
        private static readonly MethodInfo s_dumpValue = typeof(Program).GetMethod("DumpValue", BindingFlags.NonPublic | BindingFlags.Static);

        private static void DumpObject(object o)
        {
            TypeEx ty = new TypeEx(o.GetType());
            if (ty.Fields != null)
            {
                Console.WriteLine();
                foreach (FieldInfo field in ty.Fields)
                    Console.WriteLine("  {0}={1}", field.Name, field.GetValue(o));

                return;
            }

            Console.WriteLine(o);
        }

        private static void DumpValue<T>(T value)
        {
            DumpObject(value);
        }

        // Dumps the value on the top of the stack, consuming the value.
        private static void EmitDumpValue(ILGenerator g, Type ty)
        {
            MethodInfo instantiated = s_dumpValue.MakeGenericMethod(ty);
            g.Emit(OpCodes.Call, instantiated);
        }

        private static void EmitDumpArgList(ILGenerator g, IEnumerable<TypeEx> types, Action<int> emitPlaceValue, string listName)
        {
            g.Emit(OpCodes.Ldstr, $"{listName}:");
            g.Emit(OpCodes.Call, s_writeLineString);
            int index = 0;
            foreach (TypeEx ty in types)
            {
                g.Emit(OpCodes.Ldstr, $"arg{index}=");
                g.Emit(OpCodes.Call, s_writeString);

                emitPlaceValue(index);
                EmitDumpValue(g, ty.Type);
                index++;
            }
        }

        private static readonly IAbi s_abi = SelectAbi();
        private static readonly TypeEx[] s_candidateArgTypes =
            s_abi.CandidateArgTypes.Select(t => new TypeEx(t)).ToArray();

        private static IAbi SelectAbi()
        {
            Console.WriteLine("OSVersion: {0}", Environment.OSVersion);
            Console.WriteLine("OSArchitecture: {0}", RuntimeInformation.OSArchitecture);
            Console.WriteLine("ProcessArchitecture: {0}", RuntimeInformation.ProcessArchitecture);

            if (Environment.OSVersion.Platform == PlatformID.Win32NT)
            {
                if (IntPtr.Size == 8)
                {
                    Console.WriteLine("Selecting win64 ABI");
                    return new Win64Abi();
                }

                Console.WriteLine("Selecting win86 ABI");
                return new Win86Abi();
            }

            if (Environment.OSVersion.Platform == PlatformID.Unix)
            {
                Trace.Assert(IntPtr.Size == 8, "Expected 64-bit process on Unix");
                if (RuntimeInformation.ProcessArchitecture == Architecture.Arm64)
                {
                    Console.WriteLine("Selecting ARM64 ABI");
                    return new Arm64Abi();
                }

                Trace.Assert(RuntimeInformation.ProcessArchitecture == Architecture.X64);
                Console.WriteLine("Selecting SysV ABI");
                return new SysVAbi();
            }

            throw new NotSupportedException($"Platform {Environment.OSVersion.Platform} is not supported");
        }

        private class Callee
        {
            private static readonly MethodInfo s_hashCodeAddMethod =
                typeof(HashCode).GetMethods().Single(mi => mi.Name == "Add" && mi.GetParameters().Length == 1);
            private static readonly MethodInfo s_hashCodeToHashCodeMethod =
                typeof(HashCode).GetMethod("ToHashCode");

            private readonly Dictionary<CallingConvention, Type> _delegateTypes = new Dictionary<CallingConvention, Type>();

            public Callee(string name, List<TypeEx> parameters)
            {
                Name = name;
                Parameters = parameters;
                ArgStackSizeApprox = s_abi.ApproximateArgStackAreaSize(Parameters);
            }

            public string Name { get; }
            public List<TypeEx> Parameters { get; }
            public int ArgStackSizeApprox { get; }
            public DynamicMethod Method { get; private set; }
            public IReadOnlyDictionary<CallingConvention, Type> DelegateTypes => _delegateTypes;

            public void Emit()
            {
                if (Method != null)
                    return;

                Method = new DynamicMethod(
                    Name, typeof(int), Parameters.Select(t => t.Type).ToArray(), typeof(Program));

                ILGenerator g = Method.GetILGenerator();
                LocalBuilder hashCode = g.DeclareLocal(typeof(HashCode));

                if (Config.Verbose)
                    EmitDumpArgList(g, Parameters, i => g.Emit(OpCodes.Ldarg, checked((short)i)), "Callee incoming parameters");

                g.Emit(OpCodes.Ldloca, hashCode);
                g.Emit(OpCodes.Initobj, typeof(HashCode));

                for (int i = 0; i < Parameters.Count; i++)
                {
                    TypeEx pm = Parameters[i];
                    g.Emit(OpCodes.Ldloca, hashCode);
                    g.Emit(OpCodes.Ldarg, checked((short)i));
                    g.Emit(OpCodes.Call, s_hashCodeAddMethod.MakeGenericMethod(pm.Type));
                }

                g.Emit(OpCodes.Ldloca, hashCode);
                g.Emit(OpCodes.Call, s_hashCodeToHashCodeMethod);
                g.Emit(OpCodes.Ret);

                EmitDelegates();
            }

            private static ModuleBuilder s_delegateTypesModule;
            private static ConstructorInfo s_unmanagedFunctionPointerCtor =
                typeof(UnmanagedFunctionPointerAttribute).GetConstructor(new[] { typeof(CallingConvention) });

            private void EmitDelegates()
            {
                if (s_delegateTypesModule == null)
                {
                    AssemblyBuilder delegates = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("ABIStress_Delegates"), AssemblyBuilderAccess.Run);
                    s_delegateTypesModule = delegates.DefineDynamicModule("ABIStress_Delegates");
                }

                foreach (CallingConvention cc in s_abi.CallingConventions)
                {
                    // This code is based on DelegateHelpers.cs in System.Linq.Expressions.Compiler
                    TypeBuilder tb =
                        s_delegateTypesModule.DefineType(
                            $"{Name}_Delegate_{cc}",
                            TypeAttributes.Class | TypeAttributes.Public | TypeAttributes.Sealed | TypeAttributes.AutoClass,
                            typeof(MulticastDelegate));

                    tb.DefineConstructor(
                        MethodAttributes.Public | MethodAttributes.HideBySig | MethodAttributes.RTSpecialName,
                        CallingConventions.Standard,
                        new[] { typeof(object), typeof(IntPtr) })
                      .SetImplementationFlags(MethodImplAttributes.Runtime | MethodImplAttributes.Managed);

                    tb.DefineMethod(
                        "Invoke",
                        MethodAttributes.Public | MethodAttributes.HideBySig | MethodAttributes.NewSlot | MethodAttributes.Virtual,
                        typeof(int),
                        Parameters.Select(t => t.Type).ToArray())
                      .SetImplementationFlags(MethodImplAttributes.Runtime | MethodImplAttributes.Managed);

                    tb.SetCustomAttribute(new CustomAttributeBuilder(s_unmanagedFunctionPointerCtor, new object[] { cc }));
                    _delegateTypes.Add(cc, tb.CreateType());
                }
            }
        }
    }
}
