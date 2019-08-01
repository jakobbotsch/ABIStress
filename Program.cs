using Microsoft.VisualBasic;
using Microsoft.VisualBasic.CompilerServices;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.InteropServices;
using System.Threading;

namespace TailcallStress
{
    internal class Program
    {
        internal const string CallerPrefix = "TCStress_Caller";
        internal const string CalleePrefix = "TCStress_Callee";

        private static int Main(string[] args)
        {
            // Create callees. We do not emit the bodies yet, but we do need the parameter lists.
            // These will be used to select appopriate callees when generating callers below, making
            // sure that each caller has more stack arg space than each callee.
            const int numCallees = 10000;
            List<TailCallee> callees = Enumerable.Range(0, numCallees).Select(CreateCallee).ToList();

            using var tcel = new TailCallEventListener();

            int mismatches = 0;
            if (args.Length > 0 && int.TryParse(args[0], out int index))
            {
                if (!TryTailCall(index, callees))
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

                for (int i = 0; i < 1000000 && !abortLoop; i++)
                {
                    if (!TryTailCall(i, callees))
                        mismatches++;

                    if (i % 50 == 0)
                        Console.Title = $"{tcel.NumCallersSeen} callers emitted, {tcel.NumSuccessfulTailCalls} tailcalls tested";
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
                        Console.WriteLine("[{0:00.00}]: {1}", count / (double)numRejected * 100, reason);
                }
            }

            return 100 + mismatches;
        }

        private static TailCallee CreateCallee(int calleeIndex)
        {
            string name = CalleePrefix + calleeIndex;
            Random rand = new Random(0xdadbeef + calleeIndex);
            List<TypeEx> pms = RandomParameters(rand);
            var tc = new TailCallee(name, pms, typeof(long));
            return tc;
        }

        private static List<TypeEx> RandomParameters(Random rand)
        {
            List<TypeEx> pms = new List<TypeEx>(rand.Next(1, 7));
            for (int j = 0; j < pms.Capacity; j++)
                pms.Add(s_candidateArgTypes[rand.Next(s_candidateArgTypes.Length)]);

            return pms;
        }

        private static bool TryTailCall(int callerIndex, List<TailCallee> callees)
        {
            // Use a known starting seed so we can test a single caller easily.
            Random rand = new Random(0xeadbeef + callerIndex);
            List<TypeEx> pms = RandomParameters(rand);
            // Get candidate callees. It is a hard requirement that the caller has more stack space.
            int argStackSizeApprox = s_abi.ApproximateArgStackAreaSize(pms);
            List<TailCallee> callable = callees.Where(t => t.ArgStackSizeApprox < argStackSizeApprox).ToList();
            if (callable.Count <= 0)
                return true;

            int calleeIndex = rand.Next(callable.Count);
            // We might not have emitted this callee yet, so do that if so.
            if (callable[calleeIndex].Method == null)
            {
                callable[calleeIndex].Emit();
                Debug.Assert(callable[calleeIndex].Method != null);
            }

            TailCallee callee = callable[calleeIndex];

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
                    args.Add(new ConstantValue(targetTy, GenConstant(targetTy.Type, targetTy.Fields, rand)));
                }
            }

            DynamicMethod caller = new DynamicMethod(
                CallerPrefix + callerIndex, callee.ReturnType, pms.Select(t => t.Type).ToArray(), typeof(Program).Module);

            ILGenerator g = caller.GetILGenerator();
            for (int j = 0; j < args.Count; j++)
                args[j].Emit(g);

            g.Emit(OpCodes.Tailcall);
            g.EmitCall(OpCodes.Call, callee.Method, null);
            g.Emit(OpCodes.Ret);

            object[] outerArgs = pms.Select(t => GenConstant(t.Type, t.Fields, rand)).ToArray();
            object[] innerArgs = args.Select(v => v.Get(outerArgs)).ToArray();
            object result = caller.Invoke(null, outerArgs);
            object expectedResult = callee.Method.Invoke(null, innerArgs);

            if (expectedResult.Equals(result))
                return true;

            Console.WriteLine("Mismatch {0} -> {1} (expected {2}, got {3})", CallerPrefix + callerIndex, callee.Name, expectedResult, result);
            return false;
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

        private static object GenConstant(Type type, FieldInfo[] fields, Random rand)
        {
            if (type == typeof(byte))
                return (byte)rand.Next(byte.MinValue, byte.MaxValue + 1);

            if (type == typeof(short))
                return (short)rand.Next(short.MinValue, short.MaxValue + 1);

            if (type == typeof(int))
                return (int)rand.Next();

            if (type == typeof(long))
                return ((long)rand.Next() << 32) | (uint)rand.Next();

            Debug.Assert(fields != null);
            return Activator.CreateInstance(type, fields.Select(fi => GenConstant(fi.FieldType, null, rand)).ToArray());
        }

        private static readonly IAbi s_abi = SelectAbi();
        private static readonly TypeEx[] s_candidateArgTypes =
            s_abi.CandidateArgTypes.Select(t => new TypeEx(t)).ToArray();

        private static IAbi SelectAbi()
        {
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
                Trace.Assert(IntPtr.Size == 8);
                Console.WriteLine("Selecting SysV ABI");
                return new SysVAbi();
            }

            throw new NotSupportedException($"Platform {Environment.OSVersion.Platform} is not supported");
        }

        private class TailCallee
        {
            public TailCallee(string name, List<TypeEx> parameters, Type returnType)
            {
                Name = name;
                Parameters = parameters;
                ArgStackSizeApprox = s_abi.ApproximateArgStackAreaSize(Parameters);
                ReturnType = returnType;
            }

            public string Name { get; }
            public List<TypeEx> Parameters { get; }
            public int ArgStackSizeApprox { get; }
            public Type ReturnType { get; }
            public DynamicMethod Method { get; private set; }

            public void Emit()
            {
                if (Method != null)
                    return;

                Method = new DynamicMethod(
                    Name, typeof(long), Parameters.Select(t => t.Type).ToArray(), typeof(Program));

                ILGenerator g = Method.GetILGenerator();
                g.Emit(OpCodes.Ldc_I8, (long)0);
                for (int i = 0; i < Parameters.Count; i++)
                {
                    TypeEx pm = Parameters[i];
                    ArgValue arg = new ArgValue(pm, i);
                    if (pm.Fields == null)
                    {
                        arg.Emit(g);
                        g.Emit(OpCodes.Conv_I8);
                        g.Emit(OpCodes.Add);
                    }
                    else
                    {
                        for (int j = 0; j < pm.Fields.Length; j++)
                        {
                            Debug.Assert(pm.Fields[j].FieldType.IsPrimitive);
                            new FieldValue(arg, j).Emit(g);
                            g.Emit(OpCodes.Conv_I8);
                            g.Emit(OpCodes.Add);
                        }
                    }

                }

                g.Emit(OpCodes.Ret);
            }
        }

        private abstract class Value
        {
            public Value(TypeEx type)
            {
                Type = type;
            }

            public TypeEx Type { get; }

            public abstract object Get(object[] args);
            public abstract void Emit(ILGenerator il);
        }

        private class ArgValue : Value
        {
            public ArgValue(TypeEx type, int index) : base(type)
            {
                Index = index;
            }

            public int Index { get; }

            public override object Get(object[] args) => args[Index];
            public override void Emit(ILGenerator il)
            {
                il.Emit(OpCodes.Ldarg, checked((short)Index));
            }
        }

        private class FieldValue : Value
        {
            public FieldValue(Value val, int fieldIndex) : base(new TypeEx(val.Type.Fields[fieldIndex].FieldType))
            {
                Value = val;
                FieldIndex = fieldIndex;
            }

            public Value Value { get; }
            public int FieldIndex { get; }

            public override object Get(object[] args)
            {
                object value = Value.Get(args);
                value = Value.Type.Fields[FieldIndex].GetValue(value);
                return value;
            }

            public override void Emit(ILGenerator il)
            {
                Value.Emit(il);
                il.Emit(OpCodes.Ldfld, Value.Type.Fields[FieldIndex]);
            }
        }

        private class ConstantValue : Value
        {
            public ConstantValue(TypeEx type, object value) : base(type)
            {
                Value = value;
            }

            public object Value { get; }

            public override object Get(object[] args) => Value;
            public override void Emit(ILGenerator il)
            {
                if (Type.Fields == null)
                {
                    EmitLoadPrimitive(il, Value);
                    return;
                }

                foreach (FieldInfo field in Type.Fields)
                    EmitLoadPrimitive(il, field.GetValue(Value));

                il.Emit(OpCodes.Newobj, Type.Ctor);
            }

            private static void EmitLoadPrimitive(ILGenerator il, object val)
            {
                Type ty = val.GetType();
                if (ty == typeof(byte))
                    il.Emit(OpCodes.Ldc_I4, (int)(byte)val);
                else if (ty == typeof(short))
                    il.Emit(OpCodes.Ldc_I4, (int)(short)val);
                else if (ty == typeof(int))
                    il.Emit(OpCodes.Ldc_I4, (int)val);
                else if (ty == typeof(long))
                    il.Emit(OpCodes.Ldc_I8, (long)val);
                else
                    throw new NotSupportedException("Other primitives are currently not supported");
            }
        }

        private class TypeEx
        {
            public Type Type { get; }
            public int Size { get; }
            public FieldInfo[] Fields { get; }
            public ConstructorInfo Ctor { get; }

            public TypeEx(Type t)
            {
                Type = t;
                Size = Marshal.SizeOf(t);
                if (t.IsPrimitive)
                    return;

                Fields = t.GetFields().OrderBy(f => f.Name).ToArray();
                Ctor = t.GetConstructor(Fields.Select(f => f.FieldType).ToArray());
            }
        }

        private interface IAbi
        {
            Type[] CandidateArgTypes { get; }
            int ApproximateArgStackAreaSize(List<TypeEx> parameters);
        }

        private class Win86Abi : IAbi
        {
            public Type[] CandidateArgTypes { get; } =
                new[]
                {
                    typeof(byte), typeof(short), typeof(int), typeof(long),
                    typeof(S1P), typeof(S2P), typeof(S2U), typeof(S3U),
                    typeof(S4P), typeof(S4U), typeof(S5U), typeof(S6U),
                    typeof(S7U), typeof(S8P), typeof(S8U), typeof(S9U),
                    typeof(S10U), typeof(S11U), typeof(S12U), typeof(S13U),
                    typeof(S14U), typeof(S15U), typeof(S16U), typeof(S17U),
                    typeof(S31U), typeof(S32U),
                };

            public int ApproximateArgStackAreaSize(List<TypeEx> parameters)
            {
                int size = 0;
                foreach (TypeEx pm in parameters)
                    size += (pm.Size + 3) & ~3;

                return size;
            }
        }

        private class Win64Abi : IAbi
        {
            // On Win x64, only 1, 2, 4, and 8-byte sized structs can be passed on the stack.
            // Other structs will be passed by reference and will require helper.
            public Type[] CandidateArgTypes { get; } =
                new[]
                {
                    typeof(byte), typeof(short), typeof(int), typeof(long),
                    typeof(S1P), typeof(S2P), typeof(S2U), typeof(S4P),
                    typeof(S4U), typeof(S8P), typeof(S8U)
                };

            public int ApproximateArgStackAreaSize(List<TypeEx> parameters)
            {
                int size = 0;
                foreach (TypeEx pm in parameters)
                    size += (pm.Size + 7) & ~7;

                return size;
            }
        }

        private class SysVAbi : IAbi
        {
            // For SysV everything can be passed everything by value.
            public Type[] CandidateArgTypes { get; } =
                new[]
                {
                    typeof(byte), typeof(short), typeof(int), typeof(long),
                    typeof(S1P), typeof(S2P), typeof(S2U), typeof(S3U),
                    typeof(S4P), typeof(S4U), typeof(S5U), typeof(S6U),
                    typeof(S7U), typeof(S8P), typeof(S8U), typeof(S9U),
                    typeof(S10U), typeof(S11U), typeof(S12U), typeof(S13U),
                    typeof(S14U), typeof(S15U), typeof(S16U), typeof(S17U),
                    typeof(S31U), typeof(S32U),
                };

            public int ApproximateArgStackAreaSize(List<TypeEx> parameters)
            {
                int size = 0;
                foreach (TypeEx pm in parameters)
                    size += (pm.Size + 7) & ~7;

                return size;
            }
        }

        private class TailCallEventListener : EventListener
        {
            public int NumCallersSeen { get; set; }
            public int NumSuccessfulTailCalls { get; set; }
            public Dictionary<string, int> FailureReasons { get; } = new Dictionary<string, int>();

            protected override void OnEventSourceCreated(EventSource eventSource)
            {
                if (eventSource.Name != "Microsoft-Windows-DotNETRuntime")
                    return;

                EventKeywords jitTracing = (EventKeywords)0x61098; // JITSymbols | JITTracing
                EnableEvents(eventSource, EventLevel.Verbose, jitTracing);
            }

            protected override void OnEventWritten(EventWrittenEventArgs data)
            {
                string GetData(string name) => data.Payload[data.PayloadNames.IndexOf(name)].ToString();

                switch (data.EventName)
                {
                    case "MethodJitTailCallFailed":
                        if (GetData("MethodBeingCompiledName").StartsWith(CallerPrefix))
                        {
                            NumCallersSeen++;
                            string failReason = GetData("FailReason");
                            lock (FailureReasons)
                            {
                                FailureReasons[failReason] = FailureReasons.GetValueOrDefault(failReason) + 1;
                            }
                        }
                        break;
                    case "MethodJitTailCallSucceeded":
                        if (GetData("MethodBeingCompiledName").StartsWith(CallerPrefix))
                        {
                            NumCallersSeen++;
                            NumSuccessfulTailCalls++;
                        }
                        break;
                }
            }
        }
    }

    // U suffix = unpromotable, P suffix = promotable by the JIT.
    struct S1P { public byte F0; public S1P(byte f0) => F0 = f0; }
    struct S2P { public short F0; public S2P(short f0) => F0 = f0; }
    struct S2U { public byte F0, F1; public S2U(byte f0, byte f1) => (F0, F1) = (f0, f1); }
    struct S3U { public byte F0, F1, F2; public S3U(byte f0, byte f1, byte f2) => (F0, F1, F2) = (f0, f1, f2); }
    struct S4P { public int F0; public S4P(int f0) => F0 = f0; }
    struct S4U { public byte F0, F1, F2, F3; public S4U(byte f0, byte f1, byte f2, byte f3) => (F0, F1, F2, F3) = (f0, f1, f2, f3); }
    struct S5U { public byte F0, F1, F2, F3, F4; public S5U(byte f0, byte f1, byte f2, byte f3, byte f4) => (F0, F1, F2, F3, F4) = (f0, f1, f2, f3, f4); }
    struct S6U { public byte F0, F1, F2, F3, F4, F5; public S6U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5) => (F0, F1, F2, F3, F4, F5) = (f0, f1, f2, f3, f4, f5); }
    struct S7U { public byte F0, F1, F2, F3, F4, F5, F6; public S7U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6) => (F0, F1, F2, F3, F4, F5, F6) = (f0, f1, f2, f3, f4, f5, f6); }
    struct S8P { public long F0; public S8P(long f0) => F0 = f0; }
    struct S8U { public byte F0, F1, F2, F3, F4, F5, F6, F7; public S8U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7) => (F0, F1, F2, F3, F4, F5, F6, F7) = (f0, f1, f2, f3, f4, f5, f6, f7); }
    struct S9U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8; public S9U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8) => (F0, F1, F2, F3, F4, F5, F6, F7, F8) = (f0, f1, f2, f3, f4, f5, f6, f7, f8); }
    struct S10U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9; public S10U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9); }
    struct S11U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10; public S11U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10); }
    struct S12U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11; public S12U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11); }
    struct S13U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12; public S13U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11, byte f12) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12); }
    struct S14U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13; public S14U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11, byte f12, byte f13) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13); }
    struct S15U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14; public S15U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11, byte f12, byte f13, byte f14) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14); }
    struct S16U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15; public S16U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11, byte f12, byte f13, byte f14, byte f15) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15); }
    struct S17U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15, F16; public S17U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11, byte f12, byte f13, byte f14, byte f15, byte f16) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15, F16) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16); }
    struct S31U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15, F16, F17, F18, F19, F20, F21, F22, F23, F24, F25, F26, F27, F28, F29, F30; public S31U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11, byte f12, byte f13, byte f14, byte f15, byte f16, byte f17, byte f18, byte f19, byte f20, byte f21, byte f22, byte f23, byte f24, byte f25, byte f26, byte f27, byte f28, byte f29, byte f30) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15, F16, F17, F18, F19, F20, F21, F22, F23, F24, F25, F26, F27, F28, F29, F30) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23, f24, f25, f26, f27, f28, f29, f30); }
    struct S32U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15, F16, F17, F18, F19, F20, F21, F22, F23, F24, F25, F26, F27, F28, F29, F30, F31; public S32U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11, byte f12, byte f13, byte f14, byte f15, byte f16, byte f17, byte f18, byte f19, byte f20, byte f21, byte f22, byte f23, byte f24, byte f25, byte f26, byte f27, byte f28, byte f29, byte f30, byte f31) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15, F16, F17, F18, F19, F20, F21, F22, F23, F24, F25, F26, F27, F28, F29, F30, F31) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23, f24, f25, f26, f27, f28, f29, f30, f31); }
}
