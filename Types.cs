﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;

namespace ABIStress
{
    internal class TypeEx
    {
        public Type Type { get; }
        public int Size { get; }
        public FieldInfo[] Fields { get; }
        public ConstructorInfo Ctor { get; }

        public TypeEx(Type t)
        {
            Type = t;
            Size = Marshal.SizeOf(Activator.CreateInstance(t));
            if (!t.IsOurStructType())
                return;

            Fields = Enumerable.Range(0, 10000).Select(i => t.GetField($"F{i}")).TakeWhile(fi => fi != null).ToArray();
            Ctor = t.GetConstructor(Fields.Select(f => f.FieldType).ToArray());
        }
    }

    // U suffix = unpromotable, P suffix = promotable by the JIT.
    // Note that fields must be named Fi with i sequential.
    internal struct S1P { public byte F0; public S1P(byte f0) => F0 = f0; }
    internal struct S2P { public short F0; public S2P(short f0) => F0 = f0; }
    internal struct S2U { public byte F0, F1; public S2U(byte f0, byte f1) => (F0, F1) = (f0, f1); }
    internal struct S3U { public byte F0, F1, F2; public S3U(byte f0, byte f1, byte f2) => (F0, F1, F2) = (f0, f1, f2); }
    internal struct S4P { public int F0; public S4P(int f0) => F0 = f0; }
    internal struct S4U { public byte F0, F1, F2, F3; public S4U(byte f0, byte f1, byte f2, byte f3) => (F0, F1, F2, F3) = (f0, f1, f2, f3); }
    internal struct S5U { public byte F0, F1, F2, F3, F4; public S5U(byte f0, byte f1, byte f2, byte f3, byte f4) => (F0, F1, F2, F3, F4) = (f0, f1, f2, f3, f4); }
    internal struct S6U { public byte F0, F1, F2, F3, F4, F5; public S6U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5) => (F0, F1, F2, F3, F4, F5) = (f0, f1, f2, f3, f4, f5); }
    internal struct S7U { public byte F0, F1, F2, F3, F4, F5, F6; public S7U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6) => (F0, F1, F2, F3, F4, F5, F6) = (f0, f1, f2, f3, f4, f5, f6); }
    internal struct S8P { public long F0; public S8P(long f0) => F0 = f0; }
    internal struct S8U { public byte F0, F1, F2, F3, F4, F5, F6, F7; public S8U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7) => (F0, F1, F2, F3, F4, F5, F6, F7) = (f0, f1, f2, f3, f4, f5, f6, f7); }
    internal struct S9U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8; public S9U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8) => (F0, F1, F2, F3, F4, F5, F6, F7, F8) = (f0, f1, f2, f3, f4, f5, f6, f7, f8); }
    internal struct S10U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9; public S10U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9); }
    internal struct S11U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10; public S11U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10); }
    internal struct S12U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11; public S12U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11); }
    internal struct S13U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12; public S13U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11, byte f12) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12); }
    internal struct S14U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13; public S14U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11, byte f12, byte f13) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13); }
    internal struct S15U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14; public S15U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11, byte f12, byte f13, byte f14) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14); }
    internal struct S16U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15; public S16U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11, byte f12, byte f13, byte f14, byte f15) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15); }
    internal struct S17U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15, F16; public S17U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11, byte f12, byte f13, byte f14, byte f15, byte f16) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15, F16) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16); }
    internal struct S31U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15, F16, F17, F18, F19, F20, F21, F22, F23, F24, F25, F26, F27, F28, F29, F30; public S31U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11, byte f12, byte f13, byte f14, byte f15, byte f16, byte f17, byte f18, byte f19, byte f20, byte f21, byte f22, byte f23, byte f24, byte f25, byte f26, byte f27, byte f28, byte f29, byte f30) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15, F16, F17, F18, F19, F20, F21, F22, F23, F24, F25, F26, F27, F28, F29, F30) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23, f24, f25, f26, f27, f28, f29, f30); }
    internal struct S32U { public byte F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15, F16, F17, F18, F19, F20, F21, F22, F23, F24, F25, F26, F27, F28, F29, F30, F31; public S32U(byte f0, byte f1, byte f2, byte f3, byte f4, byte f5, byte f6, byte f7, byte f8, byte f9, byte f10, byte f11, byte f12, byte f13, byte f14, byte f15, byte f16, byte f17, byte f18, byte f19, byte f20, byte f21, byte f22, byte f23, byte f24, byte f25, byte f26, byte f27, byte f28, byte f29, byte f30, byte f31) => (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15, F16, F17, F18, F19, F20, F21, F22, F23, F24, F25, F26, F27, F28, F29, F30, F31) = (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23, f24, f25, f26, f27, f28, f29, f30, f31); }
    internal struct Hfa1 { public float F0, F1; public Hfa1(float f0, float f1) => (F0, F1) = (f0, f1); }
    internal struct Hfa2 { public double F0, F1, F2, F3; public Hfa2(double f0, double f1, double f2, double f3) => (F0, F1, F2, F3) = (f0, f1, f2, f3); }

    internal static class TypeExtensions
    {
        public static bool IsOurStructType(this Type t)
        {
            return
                t == typeof(S1P) || t == typeof(S2P) ||
                t == typeof(S2U) || t == typeof(S3U) ||
                t == typeof(S4P) || t == typeof(S4U) ||
                t == typeof(S5U) || t == typeof(S6U) ||
                t == typeof(S7U) || t == typeof(S8P) ||
                t == typeof(S8U) || t == typeof(S9U) ||
                t == typeof(S10U) || t == typeof(S11U) ||
                t == typeof(S12U) || t == typeof(S13U) ||
                t == typeof(S14U) || t == typeof(S15U) ||
                t == typeof(S16U) || t == typeof(S17U) ||
                t == typeof(S31U) || t == typeof(S32U) ||
                t == typeof(Hfa1) || t == typeof(Hfa2);
        }
    }
}
