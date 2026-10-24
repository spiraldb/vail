
/Users/danielking/.codex/worktrees/nullable-integer-sum/vail/vail/python/vail/_native.abi3.so:	file format mach-o arm64

Disassembly of section __TEXT,__text:

00000000040adebc <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_>:
 40adebc: 6db733ed     	stp	d13, d12, [sp, #-0x90]!
 40adec0: 6d012beb     	stp	d11, d10, [sp, #0x10]
 40adec4: 6d0223e9     	stp	d9, d8, [sp, #0x20]
 40adec8: a9036ffc     	stp	x28, x27, [sp, #0x30]
 40adecc: a90467fa     	stp	x26, x25, [sp, #0x40]
 40aded0: a9055ff8     	stp	x24, x23, [sp, #0x50]
 40aded4: a90657f6     	stp	x22, x21, [sp, #0x60]
 40aded8: a9074ff4     	stp	x20, x19, [sp, #0x70]
 40adedc: a9087bfd     	stp	x29, x30, [sp, #0x80]
 40adee0: 910203fd     	add	x29, sp, #0x80
 40adee4: d10b03ff     	sub	sp, sp, #0x2c0
 40adee8: aa0203e3     	mov	x3, x2
 40adeec: aa0103e2     	mov	x2, x1
 40adef0: aa0003e1     	mov	x1, x0
 40adef4: aa0803f7     	mov	x23, x8
 40adef8: d103c3b3     	sub	x19, x29, #0xf0
 40adefc: 910583f5     	add	x21, sp, #0x160
 40adf00: 910583e0     	add	x0, sp, #0x160
 40adf04: 940003b6     	bl	0x40aeddc <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum5inputNtNtCsgjD8fICleC5_11arrow_array5types9Int32TypeEB6_>
 40adf08: 394583e8     	ldrb	w8, [sp, #0x160]
 40adf0c: 7103fd1f     	cmp	w8, #0xff
 40adf10: 54000640     	b.eq	0x40adfd8 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x11c>
 40adf14: d2800014     	mov	x20, #0x0               ; =0
 40adf18: 3cc012a0     	ldur	q0, [x21, #0x1]
 40adf1c: 3cc112a1     	ldur	q1, [x21, #0x11]
 40adf20: ad3887a0     	stp	q0, q1, [x29, #-0xf0]
 40adf24: ad4c13e0     	ldp	q0, q4, [sp, #0x180]
 40adf28: 3c81f260     	stur	q0, [x19, #0x1f]
 40adf2c: ad500be0     	ldp	q0, q2, [sp, #0x200]
 40adf30: ad098be0     	stp	q0, q2, [sp, #0x130]
 40adf34: 3dc08be0     	ldr	q0, [sp, #0x220]
 40adf38: 3d8057e0     	str	q0, [sp, #0x150]
 40adf3c: ad4e0be0     	ldp	q0, q2, [sp, #0x1c0]
 40adf40: ad078be0     	stp	q0, q2, [sp, #0xf0]
 40adf44: ad4f03e2     	ldp	q2, q0, [sp, #0x1e0]
 40adf48: ad0883e2     	stp	q2, q0, [sp, #0x110]
 40adf4c: ad788ba0     	ldp	q0, q2, [x29, #-0xf0]
 40adf50: 3c8a13e2     	stur	q2, [sp, #0xa1]
 40adf54: 3cc1f262     	ldur	q2, [x19, #0x1f]
 40adf58: ad4d07e3     	ldp	q3, q1, [sp, #0x1a0]
 40adf5c: 3d803be1     	str	q1, [sp, #0xe0]
 40adf60: ad060fe4     	stp	q4, q3, [sp, #0xc0]
 40adf64: 3c8913e0     	stur	q0, [sp, #0x91]
 40adf68: 390243e8     	strb	w8, [sp, #0x90]
 40adf6c: f9407bf6     	ldr	x22, [sp, #0xf0]
 40adf70: f9401ec8     	ldr	x8, [x22, #0x38]
 40adf74: d342fd08     	lsr	x8, x8, #2
 40adf78: d1000513     	sub	x19, x8, #0x1
 40adf7c: d37cee75     	lsl	x21, x19, #4
 40adf80: d37cfe68     	lsr	x8, x19, #60
 40adf84: 3d802fe2     	str	q2, [sp, #0xb0]
 40adf88: b50003a8     	cbnz	x8, 0x40adffc <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x140>
 40adf8c: b27defe8     	mov	x8, #0x7ffffffffffffff8 ; =9223372036854775800
 40adf90: eb0802bf     	cmp	x21, x8
 40adf94: 54000348     	b.hi	0x40adffc <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x140>
 40adf98: f90047f7     	str	x23, [sp, #0x88]
 40adf9c: b4000395     	cbz	x21, 0x40ae00c <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x150>
 40adfa0: 970b7a25     	bl	0x38c834 <__RNvCs6rREvFdRhLb_7___rustc35___rust_no_alloc_shim_is_unstable_v2>
 40adfa4: 52800114     	mov	w20, #0x8               ; =8
 40adfa8: aa1503e0     	mov	x0, x21
 40adfac: 52800101     	mov	w1, #0x8                ; =8
 40adfb0: 970b7a1d     	bl	0x38c824 <__RNvCs6rREvFdRhLb_7___rustc12___rust_alloc>
 40adfb4: b4000240     	cbz	x0, 0x40adffc <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x140>
 40adfb8: f9401ec8     	ldr	x8, [x22, #0x38]
 40adfbc: d342fd08     	lsr	x8, x8, #2
 40adfc0: d1000517     	sub	x23, x8, #0x1
 40adfc4: f9011ff3     	str	x19, [sp, #0x238]
 40adfc8: f90123e0     	str	x0, [sp, #0x240]
 40adfcc: f90127ff     	str	xzr, [sp, #0x248]
 40adfd0: b50002d7     	cbnz	x23, 0x40ae028 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x16c>
 40adfd4: 1400031f     	b	0x40aec50 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xd94>
 40adfd8: 3cc082a0     	ldur	q0, [x21, #0x8]
 40adfdc: 3c8f72a0     	stur	q0, [x21, #0xf7]
 40adfe0: 3cc182a1     	ldur	q1, [x21, #0x18]
 40adfe4: 3c817261     	stur	q1, [x19, #0x17]
 40adfe8: f940c7e8     	ldr	x8, [sp, #0x188]
 40adfec: f8027268     	stur	x8, [x19, #0x27]
 40adff0: ad0006e0     	stp	q0, q1, [x23]
 40adff4: f90012e8     	str	x8, [x23, #0x20]
 40adff8: 1400033d     	b	0x40aecec <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xe30>
 40adffc: aa1403e0     	mov	x0, x20
 40ae000: aa1503e1     	mov	x1, x21
 40ae004: 94c01207     	bl	0x70b2820 <__RNvNtCs6KVRSXc8uZF_5alloc7raw_vec12handle_error>
 40ae008: 1400035f     	b	0x40aed84 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xec8>
 40ae00c: 52800100     	mov	w0, #0x8                ; =8
 40ae010: aa1303f7     	mov	x23, x19
 40ae014: d2800013     	mov	x19, #0x0               ; =0
 40ae018: f9011ff3     	str	x19, [sp, #0x238]
 40ae01c: f90123e0     	str	x0, [sp, #0x240]
 40ae020: f90127ff     	str	xzr, [sp, #0x248]
 40ae024: b4006177     	cbz	x23, 0x40aec50 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xd94>
 40ae028: d2800014     	mov	x20, #0x0               ; =0
 40ae02c: d2800000     	mov	x0, #0x0                ; =0
 40ae030: f00270a8     	adrp	x8, 0x8ec5000 <_anon.3d62072075af281476ac1469a7760951.293+0x960>
 40ae034: 91286108     	add	x8, x8, #0xa18
 40ae038: f90007e8     	str	x8, [sp, #0x8]
 40ae03c: b0018148     	adrp	x8, 0x70d7000 <_anon.e544dd7d5901eb9bfb0b7242f813b7e9.1898+0x2d11>
 40ae040: 3dc06d00     	ldr	q0, [x8, #0x1b0]
 40ae044: 3d801fe0     	str	q0, [sp, #0x70]
 40ae048: d0018028     	adrp	x8, 0x70b4000 <dyld_stub_binder+0x70b4000>
 40ae04c: 3dc27101     	ldr	q1, [x8, #0x9c0]
 40ae050: 528007e8     	mov	w8, #0x3f               ; =63
 40ae054: 4e080d00     	dup.2d	v0, x8
 40ae058: ad0287e0     	stp	q0, q1, [sp, #0x50]
 40ae05c: 52800088     	mov	w8, #0x4                ; =4
 40ae060: 4e080d00     	dup.2d	v0, x8
 40ae064: 3d8013e0     	str	q0, [sp, #0x40]
 40ae068: 900184a8     	adrp	x8, 0x7142000 <_anon.82ab57424711442c3c0bf381cefc2c15.38+0x74>
 40ae06c: 3dc31100     	ldr	q0, [x8, #0xc40]
 40ae070: 3d800fe0     	str	q0, [sp, #0x30]
 40ae074: 900184a8     	adrp	x8, 0x7142000 <_anon.82ab57424711442c3c0bf381cefc2c15.38+0x74>
 40ae078: 3dc31500     	ldr	q0, [x8, #0xc50]
 40ae07c: 3d800be0     	str	q0, [sp, #0x20]
 40ae080: 900184a8     	adrp	x8, 0x7142000 <_anon.82ab57424711442c3c0bf381cefc2c15.38+0x74>
 40ae084: 3dc31900     	ldr	q0, [x8, #0xc60]
 40ae088: 3d8007e0     	str	q0, [sp, #0x10]
 40ae08c: d001843b     	adrp	x27, 0x7134000 <_anon.f32e12f68d1e58cd58dbedcba77c920a.357+0x2ba>
 40ae090: 1400000a     	b	0x40ae0b8 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x1fc>
 40ae094: f94123e8     	ldr	x8, [sp, #0x240]
 40ae098: 8b141108     	add	x8, x8, x20, lsl #4
 40ae09c: 52800029     	mov	w9, #0x1                ; =1
 40ae0a0: a9005909     	stp	x9, x22, [x8]
 40ae0a4: 91000694     	add	x20, x20, #0x1
 40ae0a8: f90127f4     	str	x20, [sp, #0x248]
 40ae0ac: aa1a03e0     	mov	x0, x26
 40ae0b0: eb17035f     	cmp	x26, x23
 40ae0b4: 54005ce0     	b.eq	0x40aec50 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xd94>
 40ae0b8: 9100041a     	add	x26, x0, #0x1
 40ae0bc: f94083e8     	ldr	x8, [sp, #0x100]
 40ae0c0: b4000188     	cbz	x8, 0x40ae0f0 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x234>
 40ae0c4: f94093e8     	ldr	x8, [sp, #0x120]
 40ae0c8: eb08001f     	cmp	x0, x8
 40ae0cc: 540062e2     	b.hs	0x40aed28 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xe6c>
 40ae0d0: f94087e8     	ldr	x8, [sp, #0x108]
 40ae0d4: f9408fe9     	ldr	x9, [sp, #0x118]
 40ae0d8: 8b000129     	add	x9, x9, x0
 40ae0dc: d343fd2a     	lsr	x10, x9, #3
 40ae0e0: 386a6908     	ldrb	w8, [x8, x10]
 40ae0e4: 12000929     	and	w9, w9, #0x7
 40ae0e8: 1ac92508     	lsr	w8, w8, w9
 40ae0ec: 36002988     	tbz	w8, #0x0, 0x40ae61c <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x760>
 40ae0f0: f9407be8     	ldr	x8, [sp, #0xf0]
 40ae0f4: f9401d09     	ldr	x9, [x8, #0x38]
 40ae0f8: d342fd21     	lsr	x1, x9, #2
 40ae0fc: eb01001f     	cmp	x0, x1
 40ae100: 54006322     	b.hs	0x40aed64 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xea8>
 40ae104: eb01035f     	cmp	x26, x1
 40ae108: 54006262     	b.hs	0x40aed54 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xe98>
 40ae10c: f9401908     	ldr	x8, [x8, #0x30]
 40ae110: b8a07918     	ldrsw	x24, [x8, x0, lsl #2]
 40ae114: f9407ffc     	ldr	x28, [sp, #0xf8]
 40ae118: f9405fe9     	ldr	x9, [sp, #0xb8]
 40ae11c: cb1c0300     	sub	x0, x24, x28
 40ae120: b8ba7908     	ldrsw	x8, [x8, x26, lsl #2]
 40ae124: d342fd22     	lsr	x2, x9, #2
 40ae128: cb1c0101     	sub	x1, x8, x28
 40ae12c: eb000035     	subs	x21, x1, x0
 40ae130: 54005f43     	b.lo	0x40aed18 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xe5c>
 40ae134: eb02003f     	cmp	x1, x2
 40ae138: 54005f08     	b.hi	0x40aed18 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xe5c>
 40ae13c: d37ef413     	lsl	x19, x0, #2
 40ae140: f9405bf9     	ldr	x25, [sp, #0xb0]
 40ae144: f9409be9     	ldr	x9, [sp, #0x130]
 40ae148: b40025c9     	cbz	x9, 0x40ae600 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x744>
 40ae14c: a95387e9     	ldp	x9, x1, [sp, #0x138]
 40ae150: f940a7ea     	ldr	x10, [sp, #0x148]
 40ae154: 910583e8     	add	x8, sp, #0x160
 40ae158: 8b000142     	add	x2, x10, x0
 40ae15c: aa0903e0     	mov	x0, x9
 40ae160: aa1503e3     	mov	x3, x21
 40ae164: 94b91075     	bl	0x6ef2338 <__RNvMs_NtNtCs5CGJCcmkA0m_12arrow_buffer4util18bit_chunk_iteratorNtB4_9BitChunks3new>
 40ae168: f940b3e9     	ldr	x9, [sp, #0x160]
 40ae16c: a9572fea     	ldp	x10, x11, [sp, #0x170]
 40ae170: d2800016     	mov	x22, #0x0               ; =0
 40ae174: f27adaa8     	ands	x8, x21, #0x1fffffffffffffc0
 40ae178: 540021c0     	b.eq	0x40ae5b0 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x6f4>
 40ae17c: 9240154c     	and	x12, x10, #0x3f
 40ae180: 4b0a03ed     	neg	w13, w10
 40ae184: 924015ad     	and	x13, x13, #0x3f
 40ae188: 9100212e     	add	x14, x9, #0x8
 40ae18c: 927adaaf     	and	x15, x21, #0x1fffffffffffffc0
 40ae190: cb0f03ef     	neg	x15, x15
 40ae194: 8b130330     	add	x16, x25, x19
 40ae198: 91020210     	add	x16, x16, #0x80
 40ae19c: aa0b03f1     	mov	x17, x11
 40ae1a0: 14000030     	b	0x40ae260 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x3a4>
 40ae1a4: ad7f0201     	ldp	q1, q0, [x16, #-0x20]
 40ae1a8: ad430a03     	ldp	q3, q2, [x16, #0x60]
 40ae1ac: ad7d1205     	ldp	q5, q4, [x16, #-0x60]
 40ae1b0: ad411a07     	ldp	q7, q6, [x16, #0x20]
 40ae1b4: ad7e4211     	ldp	q17, q16, [x16, #-0x40]
 40ae1b8: ad424a13     	ldp	q19, q18, [x16, #0x40]
 40ae1bc: ad7c5215     	ldp	q21, q20, [x16, #-0x80]
 40ae1c0: ad405a17     	ldp	q23, q22, [x16]
 40ae1c4: 4ea20018     	saddl2.2d	v24, v0, v2
 40ae1c8: 4ea60099     	saddl2.2d	v25, v4, v6
 40ae1cc: 4ef88738     	add.2d	v24, v25, v24
 40ae1d0: 4eb20219     	saddl2.2d	v25, v16, v18
 40ae1d4: 4eb6029a     	saddl2.2d	v26, v20, v22
 40ae1d8: 4ef98759     	add.2d	v25, v26, v25
 40ae1dc: 4ef88738     	add.2d	v24, v25, v24
 40ae1e0: 4ea30039     	saddl2.2d	v25, v1, v3
 40ae1e4: 4ea700ba     	saddl2.2d	v26, v5, v7
 40ae1e8: 4ef98759     	add.2d	v25, v26, v25
 40ae1ec: 4eb3023a     	saddl2.2d	v26, v17, v19
 40ae1f0: 4eb702bb     	saddl2.2d	v27, v21, v23
 40ae1f4: 4efa877a     	add.2d	v26, v27, v26
 40ae1f8: 4ef98759     	add.2d	v25, v26, v25
 40ae1fc: 4ef88738     	add.2d	v24, v25, v24
 40ae200: 0ea20000     	saddl.2d	v0, v0, v2
 40ae204: 0ea60082     	saddl.2d	v2, v4, v6
 40ae208: 4ee08440     	add.2d	v0, v2, v0
 40ae20c: 0eb20202     	saddl.2d	v2, v16, v18
 40ae210: 0eb60284     	saddl.2d	v4, v20, v22
 40ae214: 4ee28482     	add.2d	v2, v4, v2
 40ae218: 4ee08440     	add.2d	v0, v2, v0
 40ae21c: 0ea30021     	saddl.2d	v1, v1, v3
 40ae220: 0ea700a2     	saddl.2d	v2, v5, v7
 40ae224: 4ee18441     	add.2d	v1, v2, v1
 40ae228: 0eb30222     	saddl.2d	v2, v17, v19
 40ae22c: 0eb702a3     	saddl.2d	v3, v21, v23
 40ae230: 4ee28462     	add.2d	v2, v3, v2
 40ae234: 4ee18441     	add.2d	v1, v2, v1
 40ae238: 4ee08420     	add.2d	v0, v1, v0
 40ae23c: 4ef88400     	add.2d	v0, v0, v24
 40ae240: 5ef1b800     	addp.2d	d0, v0
 40ae244: 9e660000     	fmov	x0, d0
 40ae248: 8b160016     	add	x22, x0, x22
 40ae24c: 910021ce     	add	x14, x14, #0x8
 40ae250: d1000631     	sub	x17, x17, #0x1
 40ae254: 91040210     	add	x16, x16, #0x100
 40ae258: b10101ef     	adds	x15, x15, #0x40
 40ae25c: 54001aa0     	b.eq	0x40ae5b0 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x6f4>
 40ae260: b4001a91     	cbz	x17, 0x40ae5b0 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x6f4>
 40ae264: f85f81c0     	ldur	x0, [x14, #-0x8]
 40ae268: b40000aa     	cbz	x10, 0x40ae27c <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x3c0>
 40ae26c: 394001c1     	ldrb	w1, [x14]
 40ae270: 9acc2400     	lsr	x0, x0, x12
 40ae274: 9acd2021     	lsl	x1, x1, x13
 40ae278: aa000020     	orr	x0, x1, x0
 40ae27c: b4fffe80     	cbz	x0, 0x40ae24c <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x390>
 40ae280: b100041f     	cmn	x0, #0x1
 40ae284: 54fff900     	b.eq	0x40ae1a4 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x2e8>
 40ae288: 4e080c02     	dup.2d	v2, x0
 40ae28c: f0018421     	adrp	x1, 0x7135000 <_anon.5b9cb949f3fb1f282ffce8c60607b0e0.1+0x97>
 40ae290: 3dc00c20     	ldr	q0, [x1, #0x30]
 40ae294: 4e201c43     	and.16b	v3, v2, v0
 40ae298: 3dc39761     	ldr	q1, [x27, #0xe50]
 40ae29c: 4e211c42     	and.16b	v2, v2, v1
 40ae2a0: ad7c1604     	ldp	q4, q5, [x16, #-0x80]
 40ae2a4: 4f20a486     	sshll2.2d	v6, v4, #0x0
 40ae2a8: 0f20a484     	sshll.2d	v4, v4, #0x0
 40ae2ac: 4ee09842     	cmeq.2d	v2, v2, #0
 40ae2b0: 4e621c82     	bic.16b	v2, v4, v2
 40ae2b4: 4ee09863     	cmeq.2d	v3, v3, #0
 40ae2b8: 4e631cc3     	bic.16b	v3, v6, v3
 40ae2bc: d344fc01     	lsr	x1, x0, #4
 40ae2c0: 4e080c24     	dup.2d	v4, x1
 40ae2c4: 4e201c86     	and.16b	v6, v4, v0
 40ae2c8: 4e211c84     	and.16b	v4, v4, v1
 40ae2cc: 4f20a4a7     	sshll2.2d	v7, v5, #0x0
 40ae2d0: 0f20a4a5     	sshll.2d	v5, v5, #0x0
 40ae2d4: 4ee09884     	cmeq.2d	v4, v4, #0
 40ae2d8: 4e641ca4     	bic.16b	v4, v5, v4
 40ae2dc: 4ee098c5     	cmeq.2d	v5, v6, #0
 40ae2e0: 4e651ce5     	bic.16b	v5, v7, v5
 40ae2e4: 4ee384a3     	add.2d	v3, v5, v3
 40ae2e8: d348fc01     	lsr	x1, x0, #8
 40ae2ec: 4e080c25     	dup.2d	v5, x1
 40ae2f0: 4ee28482     	add.2d	v2, v4, v2
 40ae2f4: 4e211ca4     	and.16b	v4, v5, v1
 40ae2f8: 4e201ca5     	and.16b	v5, v5, v0
 40ae2fc: ad7d1e06     	ldp	q6, q7, [x16, #-0x60]
 40ae300: 0f20a4d0     	sshll.2d	v16, v6, #0x0
 40ae304: 4f20a4c6     	sshll2.2d	v6, v6, #0x0
 40ae308: 4ee098a5     	cmeq.2d	v5, v5, #0
 40ae30c: 4e651cc5     	bic.16b	v5, v6, v5
 40ae310: 4ee09884     	cmeq.2d	v4, v4, #0
 40ae314: 4e641e04     	bic.16b	v4, v16, v4
 40ae318: d34cfc01     	lsr	x1, x0, #12
 40ae31c: 4e080c26     	dup.2d	v6, x1
 40ae320: 4e201cd0     	and.16b	v16, v6, v0
 40ae324: 4e211cc6     	and.16b	v6, v6, v1
 40ae328: 4f20a4f1     	sshll2.2d	v17, v7, #0x0
 40ae32c: 0f20a4e7     	sshll.2d	v7, v7, #0x0
 40ae330: 4ee098c6     	cmeq.2d	v6, v6, #0
 40ae334: 4e661ce6     	bic.16b	v6, v7, v6
 40ae338: 4ee09a07     	cmeq.2d	v7, v16, #0
 40ae33c: 4e671e27     	bic.16b	v7, v17, v7
 40ae340: 4ee584e5     	add.2d	v5, v7, v5
 40ae344: 4ee384a3     	add.2d	v3, v5, v3
 40ae348: 4ee484c4     	add.2d	v4, v6, v4
 40ae34c: 4ee28482     	add.2d	v2, v4, v2
 40ae350: d350fc01     	lsr	x1, x0, #16
 40ae354: 4e080c24     	dup.2d	v4, x1
 40ae358: 4e211c85     	and.16b	v5, v4, v1
 40ae35c: 4e201c84     	and.16b	v4, v4, v0
 40ae360: ad7e1e06     	ldp	q6, q7, [x16, #-0x40]
 40ae364: 0f20a4d0     	sshll.2d	v16, v6, #0x0
 40ae368: 4f20a4c6     	sshll2.2d	v6, v6, #0x0
 40ae36c: 4ee09884     	cmeq.2d	v4, v4, #0
 40ae370: 4e641cc4     	bic.16b	v4, v6, v4
 40ae374: 4ee098a5     	cmeq.2d	v5, v5, #0
 40ae378: 4e651e05     	bic.16b	v5, v16, v5
 40ae37c: d354fc01     	lsr	x1, x0, #20
 40ae380: 4e080c26     	dup.2d	v6, x1
 40ae384: 4e201cd0     	and.16b	v16, v6, v0
 40ae388: 4e211cc6     	and.16b	v6, v6, v1
 40ae38c: 4f20a4f1     	sshll2.2d	v17, v7, #0x0
 40ae390: 0f20a4e7     	sshll.2d	v7, v7, #0x0
 40ae394: 4ee098c6     	cmeq.2d	v6, v6, #0
 40ae398: 4e661ce6     	bic.16b	v6, v7, v6
 40ae39c: 4ee09a07     	cmeq.2d	v7, v16, #0
 40ae3a0: 4e671e27     	bic.16b	v7, v17, v7
 40ae3a4: 4ee484e4     	add.2d	v4, v7, v4
 40ae3a8: d358fc01     	lsr	x1, x0, #24
 40ae3ac: 4e080c27     	dup.2d	v7, x1
 40ae3b0: 4ee584c5     	add.2d	v5, v6, v5
 40ae3b4: 4e211ce6     	and.16b	v6, v7, v1
 40ae3b8: 4e201ce7     	and.16b	v7, v7, v0
 40ae3bc: ad7f4610     	ldp	q16, q17, [x16, #-0x20]
 40ae3c0: 0f20a612     	sshll.2d	v18, v16, #0x0
 40ae3c4: 4f20a610     	sshll2.2d	v16, v16, #0x0
 40ae3c8: 4ee098e7     	cmeq.2d	v7, v7, #0
 40ae3cc: 4e671e07     	bic.16b	v7, v16, v7
 40ae3d0: 4ee098c6     	cmeq.2d	v6, v6, #0
 40ae3d4: 4e661e46     	bic.16b	v6, v18, v6
 40ae3d8: 4ee584c5     	add.2d	v5, v6, v5
 40ae3dc: 4ee284a2     	add.2d	v2, v5, v2
 40ae3e0: 4ee484e4     	add.2d	v4, v7, v4
 40ae3e4: 4ee38483     	add.2d	v3, v4, v3
 40ae3e8: d35cfc01     	lsr	x1, x0, #28
 40ae3ec: 4e080c24     	dup.2d	v4, x1
 40ae3f0: 4e201c85     	and.16b	v5, v4, v0
 40ae3f4: 4e211c84     	and.16b	v4, v4, v1
 40ae3f8: 4f20a626     	sshll2.2d	v6, v17, #0x0
 40ae3fc: 0f20a627     	sshll.2d	v7, v17, #0x0
 40ae400: 4ee09884     	cmeq.2d	v4, v4, #0
 40ae404: 4e641ce4     	bic.16b	v4, v7, v4
 40ae408: 4ee098a5     	cmeq.2d	v5, v5, #0
 40ae40c: 4e651cc5     	bic.16b	v5, v6, v5
 40ae410: d360fc01     	lsr	x1, x0, #32
 40ae414: 4e080c26     	dup.2d	v6, x1
 40ae418: 4e211cc7     	and.16b	v7, v6, v1
 40ae41c: 4e201cc6     	and.16b	v6, v6, v0
 40ae420: ad404610     	ldp	q16, q17, [x16]
 40ae424: 0f20a612     	sshll.2d	v18, v16, #0x0
 40ae428: 4f20a610     	sshll2.2d	v16, v16, #0x0
 40ae42c: 4ee098c6     	cmeq.2d	v6, v6, #0
 40ae430: 4e661e06     	bic.16b	v6, v16, v6
 40ae434: 4ee098e7     	cmeq.2d	v7, v7, #0
 40ae438: 4e671e47     	bic.16b	v7, v18, v7
 40ae43c: 4ee484e4     	add.2d	v4, v7, v4
 40ae440: 4ee584c5     	add.2d	v5, v6, v5
 40ae444: d364fc01     	lsr	x1, x0, #36
 40ae448: 4e080c26     	dup.2d	v6, x1
 40ae44c: 4e201cc7     	and.16b	v7, v6, v0
 40ae450: 4e211cc6     	and.16b	v6, v6, v1
 40ae454: 4f20a630     	sshll2.2d	v16, v17, #0x0
 40ae458: 0f20a631     	sshll.2d	v17, v17, #0x0
 40ae45c: 4ee098c6     	cmeq.2d	v6, v6, #0
 40ae460: 4e661e26     	bic.16b	v6, v17, v6
 40ae464: 4ee098e7     	cmeq.2d	v7, v7, #0
 40ae468: 4e671e07     	bic.16b	v7, v16, v7
 40ae46c: 4ee584e5     	add.2d	v5, v7, v5
 40ae470: 4ee484c4     	add.2d	v4, v6, v4
 40ae474: d368fc01     	lsr	x1, x0, #40
 40ae478: 4e080c26     	dup.2d	v6, x1
 40ae47c: 4e211cc7     	and.16b	v7, v6, v1
 40ae480: 4e201cc6     	and.16b	v6, v6, v0
 40ae484: ad414610     	ldp	q16, q17, [x16, #0x20]
 40ae488: 0f20a612     	sshll.2d	v18, v16, #0x0
 40ae48c: 4f20a610     	sshll2.2d	v16, v16, #0x0
 40ae490: 4ee098c6     	cmeq.2d	v6, v6, #0
 40ae494: 4e661e06     	bic.16b	v6, v16, v6
 40ae498: 4ee098e7     	cmeq.2d	v7, v7, #0
 40ae49c: 4e671e47     	bic.16b	v7, v18, v7
 40ae4a0: 4ee484e4     	add.2d	v4, v7, v4
 40ae4a4: 4ee28482     	add.2d	v2, v4, v2
 40ae4a8: 4ee584c4     	add.2d	v4, v6, v5
 40ae4ac: 4ee38483     	add.2d	v3, v4, v3
 40ae4b0: d36cfc01     	lsr	x1, x0, #44
 40ae4b4: 4e080c24     	dup.2d	v4, x1
 40ae4b8: 4e201c85     	and.16b	v5, v4, v0
 40ae4bc: 4e211c84     	and.16b	v4, v4, v1
 40ae4c0: 4f20a626     	sshll2.2d	v6, v17, #0x0
 40ae4c4: 0f20a627     	sshll.2d	v7, v17, #0x0
 40ae4c8: 4ee09884     	cmeq.2d	v4, v4, #0
 40ae4cc: 4e641ce4     	bic.16b	v4, v7, v4
 40ae4d0: 4ee098a5     	cmeq.2d	v5, v5, #0
 40ae4d4: d370fc01     	lsr	x1, x0, #48
 40ae4d8: 4e080c27     	dup.2d	v7, x1
 40ae4dc: 4e651cc5     	bic.16b	v5, v6, v5
 40ae4e0: 4e211ce6     	and.16b	v6, v7, v1
 40ae4e4: 4e201ce7     	and.16b	v7, v7, v0
 40ae4e8: ad424610     	ldp	q16, q17, [x16, #0x40]
 40ae4ec: 0f20a612     	sshll.2d	v18, v16, #0x0
 40ae4f0: 4f20a610     	sshll2.2d	v16, v16, #0x0
 40ae4f4: 4ee098e7     	cmeq.2d	v7, v7, #0
 40ae4f8: 4e671e07     	bic.16b	v7, v16, v7
 40ae4fc: 4ee098c6     	cmeq.2d	v6, v6, #0
 40ae500: 4e661e46     	bic.16b	v6, v18, v6
 40ae504: 4ee484c4     	add.2d	v4, v6, v4
 40ae508: 4ee584e5     	add.2d	v5, v7, v5
 40ae50c: d374fc01     	lsr	x1, x0, #52
 40ae510: 4e080c26     	dup.2d	v6, x1
 40ae514: 4e201cc7     	and.16b	v7, v6, v0
 40ae518: 4e211cc6     	and.16b	v6, v6, v1
 40ae51c: 4f20a630     	sshll2.2d	v16, v17, #0x0
 40ae520: 0f20a631     	sshll.2d	v17, v17, #0x0
 40ae524: 4ee098c6     	cmeq.2d	v6, v6, #0
 40ae528: 4e661e26     	bic.16b	v6, v17, v6
 40ae52c: 4ee098e7     	cmeq.2d	v7, v7, #0
 40ae530: 4e671e07     	bic.16b	v7, v16, v7
 40ae534: 4ee584e5     	add.2d	v5, v7, v5
 40ae538: 4ee484c4     	add.2d	v4, v6, v4
 40ae53c: d378fc01     	lsr	x1, x0, #56
 40ae540: 4e080c26     	dup.2d	v6, x1
 40ae544: 4e201cc7     	and.16b	v7, v6, v0
 40ae548: ad434610     	ldp	q16, q17, [x16, #0x60]
 40ae54c: 4f20a612     	sshll2.2d	v18, v16, #0x0
 40ae550: 4ee098e7     	cmeq.2d	v7, v7, #0
 40ae554: 4e671e47     	bic.16b	v7, v18, v7
 40ae558: 4e211cc6     	and.16b	v6, v6, v1
 40ae55c: 0f20a610     	sshll.2d	v16, v16, #0x0
 40ae560: 4ee098c6     	cmeq.2d	v6, v6, #0
 40ae564: 4e661e06     	bic.16b	v6, v16, v6
 40ae568: 4ee484c4     	add.2d	v4, v6, v4
 40ae56c: 4ee584e5     	add.2d	v5, v7, v5
 40ae570: d37cfc00     	lsr	x0, x0, #60
 40ae574: 4e080c06     	dup.2d	v6, x0
 40ae578: 4e201cc0     	and.16b	v0, v6, v0
 40ae57c: 4e211cc1     	and.16b	v1, v6, v1
 40ae580: 4f20a626     	sshll2.2d	v6, v17, #0x0
 40ae584: 0f20a627     	sshll.2d	v7, v17, #0x0
 40ae588: 4ee09821     	cmeq.2d	v1, v1, #0
 40ae58c: 4e611ce1     	bic.16b	v1, v7, v1
 40ae590: 4ee09800     	cmeq.2d	v0, v0, #0
 40ae594: 4e601cc0     	bic.16b	v0, v6, v0
 40ae598: 4ee58400     	add.2d	v0, v0, v5
 40ae59c: 4ee38400     	add.2d	v0, v0, v3
 40ae5a0: 4ee48421     	add.2d	v1, v1, v4
 40ae5a4: 4ee28421     	add.2d	v1, v1, v2
 40ae5a8: 4ee08420     	add.2d	v0, v1, v0
 40ae5ac: 17ffff25     	b	0x40ae240 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x384>
 40ae5b0: 924016ac     	and	x12, x21, #0x3f
 40ae5b4: b4002a0c     	cbz	x12, 0x40aeaf4 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xc38>
 40ae5b8: f940c3ed     	ldr	x13, [sp, #0x180]
 40ae5bc: b40029cd     	cbz	x13, 0x40aeaf4 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xc38>
 40ae5c0: 8b0a01af     	add	x15, x13, x10
 40ae5c4: d343fdee     	lsr	x14, x15, #3
 40ae5c8: f24009ff     	tst	x15, #0x7
 40ae5cc: 1a9f07ef     	cset	w15, ne
 40ae5d0: 9a8e05d0     	cinc	x16, x14, ne
 40ae5d4: d37df16b     	lsl	x11, x11, #3
 40ae5d8: 386b6931     	ldrb	w17, [x9, x11]
 40ae5dc: 9aca2621     	lsr	x1, x17, x10
 40ae5e0: f1000a1f     	cmp	x16, #0x2
 40ae5e4: 540020c3     	b.lo	0x40ae9fc <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xb40>
 40ae5e8: 8b0f01ce     	add	x14, x14, x15
 40ae5ec: d10005cf     	sub	x15, x14, #0x1
 40ae5f0: f10011ff     	cmp	x15, #0x4
 40ae5f4: 54000442     	b.hs	0x40ae67c <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x7c0>
 40ae5f8: 52800020     	mov	w0, #0x1                ; =1
 40ae5fc: 140000f5     	b	0x40ae9d0 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xb14>
 40ae600: 6b08031f     	cmp	w24, w8
 40ae604: 54000281     	b.ne	0x40ae654 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x798>
 40ae608: d2800016     	mov	x22, #0x0               ; =0
 40ae60c: f9411fe8     	ldr	x8, [sp, #0x238]
 40ae610: eb08029f     	cmp	x20, x8
 40ae614: 54ffd401     	b.ne	0x40ae094 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x1d8>
 40ae618: 1400013b     	b	0x40aeb04 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xc48>
 40ae61c: f9411fe8     	ldr	x8, [sp, #0x238]
 40ae620: eb08029f     	cmp	x20, x8
 40ae624: 54000061     	b.ne	0x40ae630 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x774>
 40ae628: 9108e3e0     	add	x0, sp, #0x238
 40ae62c: 94be5100     	bl	0x7042a2c <__RNvMs4_NtCs6KVRSXc8uZF_5alloc7raw_vecINtB5_6RawVecTRNtNtCsfaCj5q3RDxu_14vail_execution11expressions4ExprbEE8grow_oneBS_>
 40ae630: f94123e8     	ldr	x8, [sp, #0x240]
 40ae634: d37cee89     	lsl	x9, x20, #4
 40ae638: f829691f     	str	xzr, [x8, x9]
 40ae63c: 91000694     	add	x20, x20, #0x1
 40ae640: f90127f4     	str	x20, [sp, #0x248]
 40ae644: aa1a03e0     	mov	x0, x26
 40ae648: eb17035f     	cmp	x26, x23
 40ae64c: 54ffd361     	b.ne	0x40ae0b8 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x1fc>
 40ae650: 14000180     	b	0x40aec50 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xd94>
 40ae654: f1000abf     	cmp	x21, #0x2
 40ae658: 54000082     	b.hs	0x40ae668 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x7ac>
 40ae65c: d2800009     	mov	x9, #0x0                ; =0
 40ae660: d2800016     	mov	x22, #0x0               ; =0
 40ae664: 1400003a     	b	0x40ae74c <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x890>
 40ae668: f10042bf     	cmp	x21, #0x10
 40ae66c: 54000142     	b.hs	0x40ae694 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x7d8>
 40ae670: d2800009     	mov	x9, #0x0                ; =0
 40ae674: d2800016     	mov	x22, #0x0               ; =0
 40ae678: 14000024     	b	0x40ae708 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x84c>
 40ae67c: 4e080d40     	dup.2d	v0, x10
 40ae680: f10041ff     	cmp	x15, #0x10
 40ae684: 540007a2     	b.hs	0x40ae778 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x8bc>
 40ae688: d2800010     	mov	x16, #0x0               ; =0
 40ae68c: 52800031     	mov	w17, #0x1               ; =1
 40ae690: 140000a6     	b	0x40ae928 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xa6c>
 40ae694: 927f0aaa     	and	x10, x21, #0xe
 40ae698: 927ceea9     	and	x9, x21, #0xfffffffffffffff0
 40ae69c: 8b13032b     	add	x11, x25, x19
 40ae6a0: 9100816b     	add	x11, x11, #0x20
 40ae6a4: 6f00e400     	movi.2d	v0, #0000000000000000
 40ae6a8: 927ceeac     	and	x12, x21, #0xfffffffffffffff0
 40ae6ac: 6f00e401     	movi.2d	v1, #0000000000000000
 40ae6b0: 6f00e402     	movi.2d	v2, #0000000000000000
 40ae6b4: 6f00e403     	movi.2d	v3, #0000000000000000
 40ae6b8: ad7f1564     	ldp	q4, q5, [x11, #-0x20]
 40ae6bc: acc21d66     	ldp	q6, q7, [x11], #0x40
 40ae6c0: 0ea41000     	saddw.2d	v0, v0, v4
 40ae6c4: 4ea41000     	saddw2.2d	v0, v0, v4
 40ae6c8: 0ea51021     	saddw.2d	v1, v1, v5
 40ae6cc: 4ea51021     	saddw2.2d	v1, v1, v5
 40ae6d0: 0ea61042     	saddw.2d	v2, v2, v6
 40ae6d4: 4ea61042     	saddw2.2d	v2, v2, v6
 40ae6d8: 0ea71063     	saddw.2d	v3, v3, v7
 40ae6dc: 4ea71063     	saddw2.2d	v3, v3, v7
 40ae6e0: f100418c     	subs	x12, x12, #0x10
 40ae6e4: 54fffea1     	b.ne	0x40ae6b8 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x7fc>
 40ae6e8: 4ee08420     	add.2d	v0, v1, v0
 40ae6ec: 4ee28461     	add.2d	v1, v3, v2
 40ae6f0: 4ee08420     	add.2d	v0, v1, v0
 40ae6f4: 5ef1b800     	addp.2d	d0, v0
 40ae6f8: 9e660016     	fmov	x22, d0
 40ae6fc: eb0902bf     	cmp	x21, x9
 40ae700: 54001fc0     	b.eq	0x40aeaf8 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xc3c>
 40ae704: b400024a     	cbz	x10, 0x40ae74c <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x890>
 40ae708: aa0903eb     	mov	x11, x9
 40ae70c: 6f00e400     	movi.2d	v0, #0000000000000000
 40ae710: 4e081ec0     	mov.d	v0[0], x22
 40ae714: 927ffaa9     	and	x9, x21, #0xfffffffffffffffe
 40ae718: cb09016a     	sub	x10, x11, x9
 40ae71c: d37ef56b     	lsl	x11, x11, #2
 40ae720: 8b18096b     	add	x11, x11, x24, lsl #2
 40ae724: cb1c096b     	sub	x11, x11, x28, lsl #2
 40ae728: 8b0b032b     	add	x11, x25, x11
 40ae72c: fc408561     	ldr	d1, [x11], #0x8
 40ae730: 0ea11000     	saddw.2d	v0, v0, v1
 40ae734: b100094a     	adds	x10, x10, #0x2
 40ae738: 54ffffa1     	b.ne	0x40ae72c <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x870>
 40ae73c: 5ef1b800     	addp.2d	d0, v0
 40ae740: 9e660016     	fmov	x22, d0
 40ae744: eb0902bf     	cmp	x21, x9
 40ae748: 54001d80     	b.eq	0x40aeaf8 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xc3c>
 40ae74c: 8b18012a     	add	x10, x9, x24
 40ae750: cb080148     	sub	x8, x10, x8
 40ae754: d37ef529     	lsl	x9, x9, #2
 40ae758: 8b180929     	add	x9, x9, x24, lsl #2
 40ae75c: cb1c0929     	sub	x9, x9, x28, lsl #2
 40ae760: 8b090329     	add	x9, x25, x9
 40ae764: b880452a     	ldrsw	x10, [x9], #0x4
 40ae768: 8b0a02d6     	add	x22, x22, x10
 40ae76c: b1000508     	adds	x8, x8, #0x1
 40ae770: 54ffffa3     	b.lo	0x40ae764 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x8a8>
 40ae774: 140000e1     	b	0x40aeaf8 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xc3c>
 40ae778: 927e05e0     	and	x0, x15, #0xc
 40ae77c: 927cedf0     	and	x16, x15, #0xfffffffffffffff0
 40ae780: b2400211     	orr	x17, x16, #0x1
 40ae784: 6f00e401     	movi.2d	v1, #0000000000000000
 40ae788: 4e081c21     	mov.d	v1[0], x1
 40ae78c: 6f00e402     	movi.2d	v2, #0000000000000000
 40ae790: 8b0b0121     	add	x1, x9, x11
 40ae794: 91000421     	add	x1, x1, #0x1
 40ae798: 900184a2     	adrp	x2, 0x7142000 <_anon.82ab57424711442c3c0bf381cefc2c15.38+0x74>
 40ae79c: 3dc31c43     	ldr	q3, [x2, #0xc70]
 40ae7a0: 900184a2     	adrp	x2, 0x7142000 <_anon.82ab57424711442c3c0bf381cefc2c15.38+0x74>
 40ae7a4: 3dc32044     	ldr	q4, [x2, #0xc80]
 40ae7a8: 900184a2     	adrp	x2, 0x7142000 <_anon.82ab57424711442c3c0bf381cefc2c15.38+0x74>
 40ae7ac: 3dc32445     	ldr	q5, [x2, #0xc90]
 40ae7b0: 900184a2     	adrp	x2, 0x7142000 <_anon.82ab57424711442c3c0bf381cefc2c15.38+0x74>
 40ae7b4: 3dc32846     	ldr	q6, [x2, #0xca0]
 40ae7b8: 3dc39767     	ldr	q7, [x27, #0xe50]
 40ae7bc: 927cede2     	and	x2, x15, #0xfffffffffffffff0
 40ae7c0: 6f00e410     	movi.2d	v16, #0000000000000000
 40ae7c4: 6f00e411     	movi.2d	v17, #0000000000000000
 40ae7c8: 6f00e414     	movi.2d	v20, #0000000000000000
 40ae7cc: 6f00e412     	movi.2d	v18, #0000000000000000
 40ae7d0: 6f00e415     	movi.2d	v21, #0000000000000000
 40ae7d4: 6f00e413     	movi.2d	v19, #0000000000000000
 40ae7d8: ad40dff6     	ldp	q22, q23, [sp, #0x10]
 40ae7dc: 3dc00ff8     	ldr	q24, [sp, #0x30]
 40ae7e0: 528007e3     	mov	w3, #0x3f               ; =63
 40ae7e4: 52800204     	mov	w4, #0x10               ; =16
 40ae7e8: 3cc10439     	ldr	q25, [x1], #0x10
 40ae7ec: 2f08a73a     	ushll.8h	v26, v25, #0x0
 40ae7f0: 6f08a739     	ushll2.8h	v25, v25, #0x0
 40ae7f4: 6f10a73b     	ushll2.4s	v27, v25, #0x0
 40ae7f8: 4f43549c     	shl.2d	v28, v4, #0x3
 40ae7fc: 4f4354bd     	shl.2d	v29, v5, #0x3
 40ae800: 4f4354de     	shl.2d	v30, v6, #0x3
 40ae804: 4f43547f     	shl.2d	v31, v3, #0x3
 40ae808: 4f4354e8     	shl.2d	v8, v7, #0x3
 40ae80c: 4f4356c9     	shl.2d	v9, v22, #0x3
 40ae810: 4f4356ea     	shl.2d	v10, v23, #0x3
 40ae814: 4f43570b     	shl.2d	v11, v24, #0x3
 40ae818: 6ee0856b     	sub.2d	v11, v11, v0
 40ae81c: 6ee0854a     	sub.2d	v10, v10, v0
 40ae820: 6ee08529     	sub.2d	v9, v9, v0
 40ae824: 6ee08508     	sub.2d	v8, v8, v0
 40ae828: 6ee087ff     	sub.2d	v31, v31, v0
 40ae82c: 6ee087de     	sub.2d	v30, v30, v0
 40ae830: 6ee087bd     	sub.2d	v29, v29, v0
 40ae834: 6ee0879c     	sub.2d	v28, v28, v0
 40ae838: 4e080c6c     	dup.2d	v12, x3
 40ae83c: 4e2c1f9c     	and.16b	v28, v28, v12
 40ae840: 4e2c1fbd     	and.16b	v29, v29, v12
 40ae844: 4e2c1fde     	and.16b	v30, v30, v12
 40ae848: 4e2c1fff     	and.16b	v31, v31, v12
 40ae84c: 4e2c1d08     	and.16b	v8, v8, v12
 40ae850: 4e2c1d29     	and.16b	v9, v9, v12
 40ae854: 4e2c1d4a     	and.16b	v10, v10, v12
 40ae858: 4e2c1d6b     	and.16b	v11, v11, v12
 40ae85c: 6f20a76c     	ushll2.2d	v12, v27, #0x0
 40ae860: 6eeb458b     	ushl.2d	v11, v12, v11
 40ae864: 2f10a74c     	ushll.4s	v12, v26, #0x0
 40ae868: 2f10a739     	ushll.4s	v25, v25, #0x0
 40ae86c: 2f20a77b     	ushll.2d	v27, v27, #0x0
 40ae870: 6eea477b     	ushl.2d	v27, v27, v10
 40ae874: 6f20a72a     	ushll2.2d	v10, v25, #0x0
 40ae878: 6ee94549     	ushl.2d	v9, v10, v9
 40ae87c: 2f20a58a     	ushll.2d	v10, v12, #0x0
 40ae880: 6ee84548     	ushl.2d	v8, v10, v8
 40ae884: 2f20a739     	ushll.2d	v25, v25, #0x0
 40ae888: 6eff4739     	ushl.2d	v25, v25, v31
 40ae88c: 6f10a75a     	ushll2.4s	v26, v26, #0x0
 40ae890: 6f20a59f     	ushll2.2d	v31, v12, #0x0
 40ae894: 6efe47fe     	ushl.2d	v30, v31, v30
 40ae898: 2f20a75f     	ushll.2d	v31, v26, #0x0
 40ae89c: 6efd47fd     	ushl.2d	v29, v31, v29
 40ae8a0: 6f20a75a     	ushll2.2d	v26, v26, #0x0
 40ae8a4: 6efc475a     	ushl.2d	v26, v26, v28
 40ae8a8: 4eb11f51     	orr.16b	v17, v26, v17
 40ae8ac: 4ea21fa2     	orr.16b	v2, v29, v2
 40ae8b0: 4eb01fd0     	orr.16b	v16, v30, v16
 40ae8b4: 4eb41f34     	orr.16b	v20, v25, v20
 40ae8b8: 4ea11d01     	orr.16b	v1, v8, v1
 40ae8bc: 4eb21d32     	orr.16b	v18, v9, v18
 40ae8c0: 4eb51f75     	orr.16b	v21, v27, v21
 40ae8c4: 4eb31d73     	orr.16b	v19, v11, v19
 40ae8c8: 4e080c99     	dup.2d	v25, x4
 40ae8cc: 4ef984a5     	add.2d	v5, v5, v25
 40ae8d0: 4ef984c6     	add.2d	v6, v6, v25
 40ae8d4: 4ef984e7     	add.2d	v7, v7, v25
 40ae8d8: 4ef98484     	add.2d	v4, v4, v25
 40ae8dc: 4ef98463     	add.2d	v3, v3, v25
 40ae8e0: 4ef986d6     	add.2d	v22, v22, v25
 40ae8e4: 4ef986f7     	add.2d	v23, v23, v25
 40ae8e8: 4ef98718     	add.2d	v24, v24, v25
 40ae8ec: f1004042     	subs	x2, x2, #0x10
 40ae8f0: 54fff7c1     	b.ne	0x40ae7e8 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x92c>
 40ae8f4: 4eb41c21     	orr.16b	v1, v1, v20
 40ae8f8: 4eb51c42     	orr.16b	v2, v2, v21
 40ae8fc: 4ea21c21     	orr.16b	v1, v1, v2
 40ae900: 4eb21e02     	orr.16b	v2, v16, v18
 40ae904: 4eb31e23     	orr.16b	v3, v17, v19
 40ae908: 4ea31c42     	orr.16b	v2, v2, v3
 40ae90c: 4ea21c21     	orr.16b	v1, v1, v2
 40ae910: 6e014022     	ext.16b	v2, v1, v1, #0x8
 40ae914: 0ea21c21     	orr.8b	v1, v1, v2
 40ae918: 9e660021     	fmov	x1, d1
 40ae91c: eb1001ff     	cmp	x15, x16
 40ae920: 540006e0     	b.eq	0x40ae9fc <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xb40>
 40ae924: b4001920     	cbz	x0, 0x40aec48 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xd8c>
 40ae928: 927ef5e2     	and	x2, x15, #0xfffffffffffffffc
 40ae92c: b2400040     	orr	x0, x2, #0x1
 40ae930: 6f00e401     	movi.2d	v1, #0000000000000000
 40ae934: 6f00e402     	movi.2d	v2, #0000000000000000
 40ae938: 4e081c22     	mov.d	v2[0], x1
 40ae93c: 4e080e24     	dup.2d	v4, x17
 40ae940: ad430fe5     	ldp	q5, q3, [sp, #0x60]
 40ae944: 4ee38483     	add.2d	v3, v4, v3
 40ae948: 4ee58484     	add.2d	v4, v4, v5
 40ae94c: 8b100131     	add	x17, x9, x16
 40ae950: 8b0b0231     	add	x17, x17, x11
 40ae954: 91000631     	add	x17, x17, #0x1
 40ae958: cb020210     	sub	x16, x16, x2
 40ae95c: 6f00e431     	movi.2d	v17, #0x000000000000ff
 40ae960: ad424bf3     	ldp	q19, q18, [sp, #0x40]
 40ae964: bc404625     	ldr	s5, [x17], #0x4
 40ae968: 2f08a4a5     	ushll.8h	v5, v5, #0x0
 40ae96c: 2f10a4a5     	ushll.4s	v5, v5, #0x0
 40ae970: 6f20a4a6     	ushll2.2d	v6, v5, #0x0
 40ae974: 4e311cc6     	and.16b	v6, v6, v17
 40ae978: 2f20a4a5     	ushll.2d	v5, v5, #0x0
 40ae97c: 4e311ca5     	and.16b	v5, v5, v17
 40ae980: 4f435467     	shl.2d	v7, v3, #0x3
 40ae984: 4f435490     	shl.2d	v16, v4, #0x3
 40ae988: 6ee08610     	sub.2d	v16, v16, v0
 40ae98c: 6ee084e7     	sub.2d	v7, v7, v0
 40ae990: 4e321ce7     	and.16b	v7, v7, v18
 40ae994: 4e321e10     	and.16b	v16, v16, v18
 40ae998: 6ef044a5     	ushl.2d	v5, v5, v16
 40ae99c: 6ee744c6     	ushl.2d	v6, v6, v7
 40ae9a0: 4ea11cc1     	orr.16b	v1, v6, v1
 40ae9a4: 4ea21ca2     	orr.16b	v2, v5, v2
 40ae9a8: 4ef38463     	add.2d	v3, v3, v19
 40ae9ac: 4ef38484     	add.2d	v4, v4, v19
 40ae9b0: b1001210     	adds	x16, x16, #0x4
 40ae9b4: 54fffd81     	b.ne	0x40ae964 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xaa8>
 40ae9b8: 4ea11c40     	orr.16b	v0, v2, v1
 40ae9bc: 6e004001     	ext.16b	v1, v0, v0, #0x8
 40ae9c0: 0ea11c00     	orr.8b	v0, v0, v1
 40ae9c4: 9e660001     	fmov	x1, d0
 40ae9c8: eb0201ff     	cmp	x15, x2
 40ae9cc: 54000180     	b.eq	0x40ae9fc <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xb40>
 40ae9d0: d37df00f     	lsl	x15, x0, #3
 40ae9d4: cb0a01ea     	sub	x10, x15, x10
 40ae9d8: 8b0b000b     	add	x11, x0, x11
 40ae9dc: 8b0b0129     	add	x9, x9, x11
 40ae9e0: cb0001cb     	sub	x11, x14, x0
 40ae9e4: 3840152e     	ldrb	w14, [x9], #0x1
 40ae9e8: 9aca21ce     	lsl	x14, x14, x10
 40ae9ec: aa0101c1     	orr	x1, x14, x1
 40ae9f0: 9100214a     	add	x10, x10, #0x8
 40ae9f4: f100056b     	subs	x11, x11, #0x1
 40ae9f8: 54ffff61     	b.ne	0x40ae9e4 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xb28>
 40ae9fc: 9280000a     	mov	x10, #-0x1              ; =-1
 40aea00: 9acd2149     	lsl	x9, x10, x13
 40aea04: 8a29002d     	bic	x13, x1, x9
 40aea08: 9acc2149     	lsl	x9, x10, x12
 40aea0c: ca0901a9     	eor	x9, x13, x9
 40aea10: b100053f     	cmn	x9, #0x1
 40aea14: 540007e0     	b.eq	0x40aeb10 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xc54>
 40aea18: b40006ed     	cbz	x13, 0x40aeaf4 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xc38>
 40aea1c: 8b130329     	add	x9, x25, x19
 40aea20: 8b08092c     	add	x12, x9, x8, lsl #2
 40aea24: d2800008     	mov	x8, #0x0                ; =0
 40aea28: d2800009     	mov	x9, #0x0                ; =0
 40aea2c: d280000a     	mov	x10, #0x0               ; =0
 40aea30: d280000b     	mov	x11, #0x0               ; =0
 40aea34: f27e0eae     	ands	x14, x21, #0x3c
 40aea38: 540002c0     	b.eq	0x40aea90 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xbd4>
 40aea3c: 927e0eaf     	and	x15, x21, #0x3c
 40aea40: cb0f03ef     	neg	x15, x15
 40aea44: aa0c03f0     	mov	x16, x12
 40aea48: 934001b1     	sbfx	x17, x13, #0, #1
 40aea4c: 69400600     	ldpsw	x0, x1, [x16]
 40aea50: 8a000231     	and	x17, x17, x0
 40aea54: 8b080228     	add	x8, x17, x8
 40aea58: d34205b1     	lsl	x17, x13, #62
 40aea5c: 8a91fc31     	and	x17, x1, x17, asr #63
 40aea60: d34309a0     	lsl	x0, x13, #61
 40aea64: 69410a01     	ldpsw	x1, x2, [x16, #0x8]
 40aea68: 8a80fc20     	and	x0, x1, x0, asr #63
 40aea6c: d3440da1     	lsl	x1, x13, #60
 40aea70: 8a81fc41     	and	x1, x2, x1, asr #63
 40aea74: 8b090229     	add	x9, x17, x9
 40aea78: 8b0a000a     	add	x10, x0, x10
 40aea7c: 8b0b002b     	add	x11, x1, x11
 40aea80: 91004210     	add	x16, x16, #0x10
 40aea84: d344fdad     	lsr	x13, x13, #4
 40aea88: b10011ef     	adds	x15, x15, #0x4
 40aea8c: 54fffde1     	b.ne	0x40aea48 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xb8c>
 40aea90: d37e06af     	ubfiz	x15, x21, #2, #2
 40aea94: b400028f     	cbz	x15, 0x40aeae4 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xc28>
 40aea98: 8b0e098c     	add	x12, x12, x14, lsl #2
 40aea9c: b980018e     	ldrsw	x14, [x12]
 40aeaa0: 934001b0     	sbfx	x16, x13, #0, #1
 40aeaa4: 8a0e020e     	and	x14, x16, x14
 40aeaa8: 8b0801c8     	add	x8, x14, x8
 40aeaac: f10011ff     	cmp	x15, #0x4
 40aeab0: 540001a0     	b.eq	0x40aeae4 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xc28>
 40aeab4: b980058e     	ldrsw	x14, [x12, #0x4]
 40aeab8: d34205b0     	lsl	x16, x13, #62
 40aeabc: 8a90fdce     	and	x14, x14, x16, asr #63
 40aeac0: 8b0901c9     	add	x9, x14, x9
 40aeac4: f10021ff     	cmp	x15, #0x8
 40aeac8: 540000e0     	b.eq	0x40aeae4 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xc28>
 40aeacc: f10031ff     	cmp	x15, #0xc
 40aead0: 54001501     	b.ne	0x40aed70 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xeb4>
 40aead4: b980098c     	ldrsw	x12, [x12, #0x8]
 40aead8: d34309ad     	lsl	x13, x13, #61
 40aeadc: 8a8dfd8c     	and	x12, x12, x13, asr #63
 40aeae0: 8b0a018a     	add	x10, x12, x10
 40aeae4: 8b16016b     	add	x11, x11, x22
 40aeae8: 8b090108     	add	x8, x8, x9
 40aeaec: 8b080168     	add	x8, x11, x8
 40aeaf0: 8b0a0116     	add	x22, x8, x10
 40aeaf4: f94127f4     	ldr	x20, [sp, #0x248]
 40aeaf8: f9411fe8     	ldr	x8, [sp, #0x238]
 40aeafc: eb08029f     	cmp	x20, x8
 40aeb00: 54ffaca1     	b.ne	0x40ae094 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x1d8>
 40aeb04: 9108e3e0     	add	x0, sp, #0x238
 40aeb08: 94be4fc9     	bl	0x7042a2c <__RNvMs4_NtCs6KVRSXc8uZF_5alloc7raw_vecINtB5_6RawVecTRNtNtCsfaCj5q3RDxu_14vail_execution11expressions4ExprbEE8grow_oneBS_>
 40aeb0c: 17fffd62     	b	0x40ae094 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x1d8>
 40aeb10: d37ef6a8     	lsl	x8, x21, #2
 40aeb14: f100059f     	cmp	x12, #0x1
 40aeb18: 54000081     	b.ne	0x40aeb28 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xc6c>
 40aeb1c: d2800009     	mov	x9, #0x0                ; =0
 40aeb20: d280000b     	mov	x11, #0x0               ; =0
 40aeb24: 14000039     	b	0x40aec08 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xd4c>
 40aeb28: f100419f     	cmp	x12, #0x10
 40aeb2c: 54000082     	b.hs	0x40aeb3c <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xc80>
 40aeb30: d2800009     	mov	x9, #0x0                ; =0
 40aeb34: d280000b     	mov	x11, #0x0               ; =0
 40aeb38: 14000022     	b	0x40aebc0 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xd04>
 40aeb3c: 927f0aaa     	and	x10, x21, #0xe
 40aeb40: 927c06a9     	and	x9, x21, #0x30
 40aeb44: d346f2ab     	ubfx	x11, x21, #6, #55
 40aeb48: d378dd6b     	lsl	x11, x11, #8
 40aeb4c: 8b18096b     	add	x11, x11, x24, lsl #2
 40aeb50: cb1c096b     	sub	x11, x11, x28, lsl #2
 40aeb54: 8b0b032b     	add	x11, x25, x11
 40aeb58: 9100816b     	add	x11, x11, #0x20
 40aeb5c: 6f00e400     	movi.2d	v0, #0000000000000000
 40aeb60: 927c06ad     	and	x13, x21, #0x30
 40aeb64: 6f00e401     	movi.2d	v1, #0000000000000000
 40aeb68: 6f00e402     	movi.2d	v2, #0000000000000000
 40aeb6c: 6f00e403     	movi.2d	v3, #0000000000000000
 40aeb70: ad7f1564     	ldp	q4, q5, [x11, #-0x20]
 40aeb74: acc21d66     	ldp	q6, q7, [x11], #0x40
 40aeb78: 0ea41000     	saddw.2d	v0, v0, v4
 40aeb7c: 4ea41000     	saddw2.2d	v0, v0, v4
 40aeb80: 0ea51021     	saddw.2d	v1, v1, v5
 40aeb84: 4ea51021     	saddw2.2d	v1, v1, v5
 40aeb88: 0ea61042     	saddw.2d	v2, v2, v6
 40aeb8c: 4ea61042     	saddw2.2d	v2, v2, v6
 40aeb90: 0ea71063     	saddw.2d	v3, v3, v7
 40aeb94: 4ea71063     	saddw2.2d	v3, v3, v7
 40aeb98: f10041ad     	subs	x13, x13, #0x10
 40aeb9c: 54fffea1     	b.ne	0x40aeb70 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xcb4>
 40aeba0: 4ee08420     	add.2d	v0, v1, v0
 40aeba4: 4ee28461     	add.2d	v1, v3, v2
 40aeba8: 4ee08420     	add.2d	v0, v1, v0
 40aebac: 5ef1b800     	addp.2d	d0, v0
 40aebb0: 9e66000b     	fmov	x11, d0
 40aebb4: eb09019f     	cmp	x12, x9
 40aebb8: 540003c0     	b.eq	0x40aec30 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xd74>
 40aebbc: b400026a     	cbz	x10, 0x40aec08 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xd4c>
 40aebc0: aa0903ed     	mov	x13, x9
 40aebc4: 927f12a9     	and	x9, x21, #0x3e
 40aebc8: 6f00e400     	movi.2d	v0, #0000000000000000
 40aebcc: 4e081d60     	mov.d	v0[0], x11
 40aebd0: cb0901aa     	sub	x10, x13, x9
 40aebd4: 9278d90b     	and	x11, x8, #0x7fffffffffffff00
 40aebd8: aa0d096b     	orr	x11, x11, x13, lsl #2
 40aebdc: 8b18096b     	add	x11, x11, x24, lsl #2
 40aebe0: cb1c096b     	sub	x11, x11, x28, lsl #2
 40aebe4: 8b0b032b     	add	x11, x25, x11
 40aebe8: fc408561     	ldr	d1, [x11], #0x8
 40aebec: 0ea11000     	saddw.2d	v0, v0, v1
 40aebf0: b100094a     	adds	x10, x10, #0x2
 40aebf4: 54ffffa1     	b.ne	0x40aebe8 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xd2c>
 40aebf8: 5ef1b800     	addp.2d	d0, v0
 40aebfc: 9e66000b     	fmov	x11, d0
 40aec00: eb09019f     	cmp	x12, x9
 40aec04: 54000160     	b.eq	0x40aec30 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xd74>
 40aec08: cb09018a     	sub	x10, x12, x9
 40aec0c: 9278d908     	and	x8, x8, #0x7fffffffffffff00
 40aec10: aa090908     	orr	x8, x8, x9, lsl #2
 40aec14: 8b180908     	add	x8, x8, x24, lsl #2
 40aec18: cb1c0908     	sub	x8, x8, x28, lsl #2
 40aec1c: 8b080328     	add	x8, x25, x8
 40aec20: b8804509     	ldrsw	x9, [x8], #0x4
 40aec24: 8b09016b     	add	x11, x11, x9
 40aec28: f100054a     	subs	x10, x10, #0x1
 40aec2c: 54ffffa1     	b.ne	0x40aec20 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xd64>
 40aec30: 8b160176     	add	x22, x11, x22
 40aec34: f94127f4     	ldr	x20, [sp, #0x248]
 40aec38: f9411fe8     	ldr	x8, [sp, #0x238]
 40aec3c: eb08029f     	cmp	x20, x8
 40aec40: 54ffa2a1     	b.ne	0x40ae094 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0x1d8>
 40aec44: 17ffffb0     	b	0x40aeb04 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xc48>
 40aec48: b2400200     	orr	x0, x16, #0x1
 40aec4c: 17ffff61     	b	0x40ae9d0 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xb14>
 40aec50: 910583e8     	add	x8, sp, #0x160
 40aec54: 3ccd8100     	ldur	q0, [x8, #0xd8]
 40aec58: 3d805be0     	str	q0, [sp, #0x160]
 40aec5c: f94127e8     	ldr	x8, [sp, #0x248]
 40aec60: f900bbe8     	str	x8, [sp, #0x170]
 40aec64: 910583f4     	add	x20, sp, #0x160
 40aec68: d103c3a8     	sub	x8, x29, #0xf0
 40aec6c: 910583e0     	add	x0, sp, #0x160
 40aec70: 94b5fdcc     	bl	0x6e2e3a0 <__RNvXsB_NtNtCsgjD8fICleC5_11arrow_array5array15primitive_arrayINtB5_14PrimitiveArrayNtNtB9_5types9Int64TypeEINtNtCslWxY2MhVcag_4core7convert4FromINtNtCs6KVRSXc8uZF_5alloc3vec3VecINtNtB1M_6option6OptionxEEE4from>
 40aec74: 52800028     	mov	w8, #0x1                ; =1
 40aec78: 4e080d00     	dup.2d	v0, x8
 40aec7c: ad798ba1     	ldp	q1, q2, [x29, #-0xd0]
 40aec80: ad7a93a3     	ldp	q3, q4, [x29, #-0xb0]
 40aec84: ad0d0fe2     	stp	q2, q3, [sp, #0x1a0]
 40aec88: 3d8073e4     	str	q4, [sp, #0x1c0]
 40aec8c: ad788fa2     	ldp	q2, q3, [x29, #-0xf0]
 40aec90: ad0b0be0     	stp	q0, q2, [sp, #0x160]
 40aec94: ad0c07e3     	stp	q3, q1, [sp, #0x180]
 40aec98: 970b76e7     	bl	0x38c834 <__RNvCs6rREvFdRhLb_7___rustc35___rust_no_alloc_shim_is_unstable_v2>
 40aec9c: 52800e00     	mov	w0, #0x70               ; =112
 40aeca0: 52800101     	mov	w1, #0x8                ; =8
 40aeca4: 970b76e0     	bl	0x38c824 <__RNvCs6rREvFdRhLb_7___rustc12___rust_alloc>
 40aeca8: f94047e9     	ldr	x9, [sp, #0x88]
 40aecac: b40004c0     	cbz	x0, 0x40aed44 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xe88>
 40aecb0: ad4d07e0     	ldp	q0, q1, [sp, #0x1a0]
 40aecb4: ad020400     	stp	q0, q1, [x0, #0x40]
 40aecb8: 3dc073e0     	ldr	q0, [sp, #0x1c0]
 40aecbc: 3d801800     	str	q0, [x0, #0x60]
 40aecc0: ad4b07e0     	ldp	q0, q1, [sp, #0x160]
 40aecc4: ad000400     	stp	q0, q1, [x0]
 40aecc8: ad4c03e1     	ldp	q1, q0, [sp, #0x180]
 40aeccc: ad010001     	stp	q1, q0, [x0, #0x20]
 40aecd0: f00270a8     	adrp	x8, 0x8ec5000 <_anon.3d62072075af281476ac1469a7760951.293+0x960>
 40aecd4: 91252108     	add	x8, x8, #0x948
 40aecd8: a900a120     	stp	x0, x8, [x9, #0x8]
 40aecdc: 92800008     	mov	x8, #-0x1               ; =-1
 40aece0: f9000128     	str	x8, [x9]
 40aece4: 910243e0     	add	x0, sp, #0x90
 40aece8: 97ffe610     	bl	0x40a8528 <__RINvNtCslWxY2MhVcag_4core3ptr9drop_glueINtNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum5InputNtNtCsgjD8fICleC5_11arrow_array5types9Int32TypeEEBI_>
 40aecec: 910b03ff     	add	sp, sp, #0x2c0
 40aecf0: a9487bfd     	ldp	x29, x30, [sp, #0x80]
 40aecf4: a9474ff4     	ldp	x20, x19, [sp, #0x70]
 40aecf8: a94657f6     	ldp	x22, x21, [sp, #0x60]
 40aecfc: a9455ff8     	ldp	x24, x23, [sp, #0x50]
 40aed00: a94467fa     	ldp	x26, x25, [sp, #0x40]
 40aed04: a9436ffc     	ldp	x28, x27, [sp, #0x30]
 40aed08: 6d4223e9     	ldp	d9, d8, [sp, #0x20]
 40aed0c: 6d412beb     	ldp	d11, d10, [sp, #0x10]
 40aed10: 6cc933ed     	ldp	d13, d12, [sp], #0x90
 40aed14: d65f03c0     	ret
 40aed18: f00270a3     	adrp	x3, 0x8ec5000 <_anon.3d62072075af281476ac1469a7760951.293+0x960>
 40aed1c: 91292063     	add	x3, x3, #0xa48
 40aed20: 94c00fc5     	bl	0x70b2c34 <__RNvNtNtCslWxY2MhVcag_4core5slice5index16slice_index_fail>
 40aed24: 14000018     	b	0x40aed84 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xec8>
 40aed28: d0018c20     	adrp	x0, 0x7234000 <_anon.3d62072075af281476ac1469a7760951.267+0x95c>
 40aed2c: 911fc800     	add	x0, x0, #0x7f2
 40aed30: f00270a2     	adrp	x2, 0x8ec5000 <_anon.3d62072075af281476ac1469a7760951.293+0x960>
 40aed34: 91280042     	add	x2, x2, #0xa00
 40aed38: 52800481     	mov	w1, #0x24               ; =36
 40aed3c: 94c00faf     	bl	0x70b2bf8 <__RNvNtCslWxY2MhVcag_4core9panicking5panic>
 40aed40: 14000011     	b	0x40aed84 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xec8>
 40aed44: 52800100     	mov	w0, #0x8                ; =8
 40aed48: 52800e01     	mov	w1, #0x70               ; =112
 40aed4c: 94c00eaf     	bl	0x70b2808 <__RNvNtCs6KVRSXc8uZF_5alloc5alloc18handle_alloc_error>
 40aed50: 1400000d     	b	0x40aed84 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xec8>
 40aed54: aa1a03e0     	mov	x0, x26
 40aed58: f00270a8     	adrp	x8, 0x8ec5000 <_anon.3d62072075af281476ac1469a7760951.293+0x960>
 40aed5c: 9128c108     	add	x8, x8, #0xa30
 40aed60: f90007e8     	str	x8, [sp, #0x8]
 40aed64: f94007e2     	ldr	x2, [sp, #0x8]
 40aed68: 94c00f4d     	bl	0x70b2a9c <__RNvNtCslWxY2MhVcag_4core9panicking18panic_bounds_check>
 40aed6c: 14000006     	b	0x40aed84 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xec8>
 40aed70: 900270c2     	adrp	x2, 0x8ec6000 <_anon.0ab26eca08f227dd4bdc345d38e999b5.80+0x1c8>
 40aed74: 91112042     	add	x2, x2, #0x448
 40aed78: 52800080     	mov	w0, #0x4                ; =4
 40aed7c: 52800081     	mov	w1, #0x4                ; =4
 40aed80: 94c00f47     	bl	0x70b2a9c <__RNvNtCslWxY2MhVcag_4core9panicking18panic_bounds_check>
 40aed84: d4200020     	brk	#0x1
 40aed88: 1400000f     	b	0x40aedc4 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xf08>
 40aed8c: 14000006     	b	0x40aeda4 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xee8>
 40aed90: aa0003f3     	mov	x19, x0
 40aed94: 91004280     	add	x0, x20, #0x10
 40aed98: 97ffe618     	bl	0x40a85f8 <__RINvNtCslWxY2MhVcag_4core3ptr9drop_glueINtNtNtCsgjD8fICleC5_11arrow_array5array15primitive_array14PrimitiveArrayNtNtBI_5types11Float64TypeEECsfaCj5q3RDxu_14vail_execution>
 40aed9c: 1400000b     	b	0x40aedc8 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xf0c>
 40aeda0: 94c00f39     	bl	0x70b2a84 <__RNvNtCslWxY2MhVcag_4core9panicking16panic_in_cleanup>
 40aeda4: aa0003f3     	mov	x19, x0
 40aeda8: f9411fe8     	ldr	x8, [sp, #0x238]
 40aedac: b40000e8     	cbz	x8, 0x40aedc8 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xf0c>
 40aedb0: f94123e0     	ldr	x0, [sp, #0x240]
 40aedb4: d37ced01     	lsl	x1, x8, #4
 40aedb8: 52800102     	mov	w2, #0x8                ; =8
 40aedbc: 970b769b     	bl	0x38c828 <__RNvCs6rREvFdRhLb_7___rustc14___rust_dealloc>
 40aedc0: 14000002     	b	0x40aedc8 <__RINvNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum13evaluate_wideKb1_EB6_+0xf0c>
 40aedc4: aa0003f3     	mov	x19, x0
 40aedc8: 910243e0     	add	x0, sp, #0x90
 40aedcc: 97ffe5d7     	bl	0x40a8528 <__RINvNtCslWxY2MhVcag_4core3ptr9drop_glueINtNtNtCsfaCj5q3RDxu_14vail_execution11expressions16integer_list_sum5InputNtNtCsgjD8fICleC5_11arrow_array5types9Int32TypeEEBI_>
 40aedd0: aa1303e0     	mov	x0, x19
 40aedd4: 94c01122     	bl	0x70b325c <dyld_stub_binder+0x70b325c>
 40aedd8: 94c00f2b     	bl	0x70b2a84 <__RNvNtCslWxY2MhVcag_4core9panicking16panic_in_cleanup>
