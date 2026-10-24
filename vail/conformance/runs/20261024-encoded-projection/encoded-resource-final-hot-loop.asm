
/Users/danielking/.codex/worktrees/encoded-list-reduction/vail/vail/python/vail/_native.abi3.so:	file format mach-o arm64

Disassembly of section __TEXT,__text:

00000000021b71b0 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_>:
 21b71b0: d10383ff     	sub	sp, sp, #0xe0
 21b71b4: a90a5ff8     	stp	x24, x23, [sp, #0xa0]
 21b71b8: a90b57f6     	stp	x22, x21, [sp, #0xb0]
 21b71bc: a90c4ff4     	stp	x20, x19, [sp, #0xc0]
 21b71c0: a90d7bfd     	stp	x29, x30, [sp, #0xd0]
 21b71c4: 910343fd     	add	x29, sp, #0xd0
 21b71c8: aa0803f3     	mov	x19, x8
 21b71cc: 910123e8     	add	x8, sp, #0x48
 21b71d0: a9405016     	ldp	x22, x20, [x0]
 21b71d4: f9400817     	ldr	x23, [x0, #0x10]
 21b71d8: 3cc18001     	ldur	q1, [x0, #0x18]
 21b71dc: 3cc28000     	ldur	q0, [x0, #0x28]
 21b71e0: ad0303e1     	stp	q1, q0, [sp, #0x60]
 21b71e4: 3cc38000     	ldur	q0, [x0, #0x38]
 21b71e8: 3c838100     	stur	q0, [x8, #0x38]
 21b71ec: 3cc48000     	ldur	q0, [x0, #0x48]
 21b71f0: 3c848100     	stur	q0, [x8, #0x48]
 21b71f4: eb1402e8     	subs	x8, x23, x20
 21b71f8: 9a8833f5     	csel	x21, xzr, x8, lo
 21b71fc: 910003e8     	mov	x8, sp
 21b7200: aa1503e0     	mov	x0, x21
 21b7204: 52800061     	mov	w1, #0x3                ; =3
 21b7208: 52800022     	mov	w2, #0x1                ; =1
 21b720c: 52800103     	mov	w3, #0x8                ; =8
 21b7210: d2800004     	mov	x4, #0x0                ; =0
 21b7214: 9400432d     	bl	0x21c7ec8 <__RNvMs0_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB5_9BufferMutyE34with_capacity_preferred_aligned_inCs8Gx6krhMvpD_12vortex_array>
 21b7218: a904d3f6     	stp	x22, x20, [sp, #0x48]
 21b721c: f9002ff7     	str	x23, [sp, #0x58]
 21b7220: a9432bec     	ldp	x12, x10, [sp, #0x30]
 21b7224: cb0c0149     	sub	x9, x10, x12
 21b7228: eb0902bf     	cmp	x21, x9
 21b722c: 540000c9     	b.ls	0x21b7244 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x94>
 21b7230: 910003e0     	mov	x0, sp
 21b7234: aa1503e1     	mov	x1, x21
 21b7238: 94003ede     	bl	0x21c6db0 <__RNvMs0_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB5_9BufferMutyE16reserve_allocateCs8Gx6krhMvpD_12vortex_array>
 21b723c: a9432bec     	ldp	x12, x10, [sp, #0x30]
 21b7240: cb0c0149     	sub	x9, x10, x12
 21b7244: f94017e8     	ldr	x8, [sp, #0x28]
 21b7248: 8b0c0d08     	add	x8, x8, x12, lsl #3
 21b724c: eb0c015f     	cmp	x10, x12
 21b7250: 54000061     	b.ne	0x21b725c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0xac>
 21b7254: aa0803eb     	mov	x11, x8
 21b7258: 14000063     	b	0x21b73e4 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x234>
 21b725c: f94033ea     	ldr	x10, [sp, #0x60]
 21b7260: eb1402ff     	cmp	x23, x20
 21b7264: 54000ba9     	b.ls	0x21b73d8 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x228>
 21b7268: a946b7ec     	ldp	x12, x13, [sp, #0x68]
 21b726c: a9478bee     	ldp	x14, x2, [sp, #0x78]
 21b7270: 5280002f     	mov	w15, #0x1               ; =1
 21b7274: 52800030     	mov	w16, #0x1               ; =1
 21b7278: aa0803eb     	mov	x11, x8
 21b727c: f94047f1     	ldr	x17, [sp, #0x88]
 21b7280: f9404fe3     	ldr	x3, [sp, #0x98]
 21b7284: f94002c0     	ldr	x0, [x22]
 21b7288: b100041f     	cmn	x0, #0x1
 21b728c: 54000140     	b.eq	0x21b72b4 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x104>
 21b7290: f9400ac1     	ldr	x1, [x22, #0x10]
 21b7294: eb01029f     	cmp	x20, x1
 21b7298: 54000f62     	b.hs	0x21b7484 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x2d4>
 21b729c: f94006c0     	ldr	x0, [x22, #0x8]
 21b72a0: 8b141001     	add	x1, x0, x20, lsl #4
 21b72a4: a9401420     	ldp	x0, x5, [x1]
 21b72a8: f9400181     	ldr	x1, [x12]
 21b72ac: b5000121     	cbnz	x1, 0x21b72d0 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x120>
 21b72b0: 14000016     	b	0x21b7308 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x158>
 21b72b4: f94006c0     	ldr	x0, [x22, #0x8]
 21b72b8: eb00029f     	cmp	x20, x0
 21b72bc: 54000c02     	b.hs	0x21b743c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x28c>
 21b72c0: f9400ac5     	ldr	x5, [x22, #0x10]
 21b72c4: 9b147ca0     	mul	x0, x5, x20
 21b72c8: f9400181     	ldr	x1, [x12]
 21b72cc: b40001e1     	cbz	x1, 0x21b7308 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x158>
 21b72d0: f100043f     	cmp	x1, #0x1
 21b72d4: 540004c0     	b.eq	0x21b736c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x1bc>
 21b72d8: f9400581     	ldr	x1, [x12, #0x8]
 21b72dc: f9401c24     	ldr	x4, [x1, #0x38]
 21b72e0: eb04015f     	cmp	x10, x4
 21b72e4: 54000ba2     	b.hs	0x21b7458 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x2a8>
 21b72e8: f9400824     	ldr	x4, [x1, #0x10]
 21b72ec: f9401821     	ldr	x1, [x1, #0x30]
 21b72f0: 8b0a0021     	add	x1, x1, x10
 21b72f4: d343fc26     	lsr	x6, x1, #3
 21b72f8: 38666884     	ldrb	w4, [x4, x6]
 21b72fc: 92400821     	and	x1, x1, #0x7
 21b7300: 1ac12481     	lsr	w1, w4, w1
 21b7304: 36000341     	tbz	w1, #0x0, 0x21b736c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x1bc>
 21b7308: b4000405     	cbz	x5, 0x21b7388 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x1d8>
 21b730c: f94001a1     	ldr	x1, [x13]
 21b7310: cb010000     	sub	x0, x0, x1
 21b7314: ab050001     	adds	x1, x0, x5
 21b7318: 540008a2     	b.hs	0x21b742c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x27c>
 21b731c: eb02003f     	cmp	x1, x2
 21b7320: 54000868     	b.hi	0x21b742c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x27c>
 21b7324: d2800004     	mov	x4, #0x0                ; =0
 21b7328: 52800006     	mov	w6, #0x0                ; =0
 21b732c: d37ff8a5     	lsl	x5, x5, #1
 21b7330: f9400621     	ldr	x1, [x17, #0x8]
 21b7334: 8b0005c7     	add	x7, x14, x0, lsl #1
 21b7338: 784024e0     	ldrh	w0, [x7], #0x2
 21b733c: eb00003f     	cmp	x1, x0
 21b7340: 540009a9     	b.ls	0x21b7474 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x2c4>
 21b7344: f9400237     	ldr	x23, [x17]
 21b7348: 8b000ee0     	add	x0, x23, x0, lsl #3
 21b734c: 29400017     	ldp	w23, w0, [x0]
 21b7350: 93407ef7     	sxtw	x23, w23
 21b7354: 8b170084     	add	x4, x4, x23
 21b7358: 2a060006     	orr	w6, w0, w6
 21b735c: f10008a5     	subs	x5, x5, #0x2
 21b7360: 54fffec1     	b.ne	0x21b7338 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x188>
 21b7364: 35000266     	cbnz	w6, 0x21b73b0 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x200>
 21b7368: 14000009     	b	0x21b738c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x1dc>
 21b736c: d2800004     	mov	x4, #0x0                ; =0
 21b7370: 91000694     	add	x20, x20, #0x1
 21b7374: 9100054a     	add	x10, x10, #0x1
 21b7378: f8008564     	str	x4, [x11], #0x8
 21b737c: eb09021f     	cmp	x16, x9
 21b7380: 54000221     	b.ne	0x21b73c4 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x214>
 21b7384: 14000013     	b	0x21b73d0 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x220>
 21b7388: d2800004     	mov	x4, #0x0                ; =0
 21b738c: f9401460     	ldr	x0, [x3, #0x28]
 21b7390: f9402461     	ldr	x1, [x3, #0x48]
 21b7394: 8b0a0021     	add	x1, x1, x10
 21b7398: 92400825     	and	x5, x1, #0x7
 21b739c: 1ac521e5     	lsl	w5, w15, w5
 21b73a0: d343fc21     	lsr	x1, x1, #3
 21b73a4: 38616806     	ldrb	w6, [x0, x1]
 21b73a8: 2a0600a5     	orr	w5, w5, w6
 21b73ac: 38216805     	strb	w5, [x0, x1]
 21b73b0: 91000694     	add	x20, x20, #0x1
 21b73b4: 9100054a     	add	x10, x10, #0x1
 21b73b8: f8008564     	str	x4, [x11], #0x8
 21b73bc: eb09021f     	cmp	x16, x9
 21b73c0: 54000080     	b.eq	0x21b73d0 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x220>
 21b73c4: eb15021f     	cmp	x16, x21
 21b73c8: 91000610     	add	x16, x16, #0x1
 21b73cc: 54fff5c1     	b.ne	0x21b7284 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0xd4>
 21b73d0: f9401bec     	ldr	x12, [sp, #0x30]
 21b73d4: 14000002     	b	0x21b73dc <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x22c>
 21b73d8: aa0803eb     	mov	x11, x8
 21b73dc: f9002bf4     	str	x20, [sp, #0x50]
 21b73e0: f90033ea     	str	x10, [sp, #0x60]
 21b73e4: cb080168     	sub	x8, x11, x8
 21b73e8: 8b480d88     	add	x8, x12, x8, lsr #3
 21b73ec: f9001be8     	str	x8, [sp, #0x30]
 21b73f0: 910123e0     	add	x0, sp, #0x48
 21b73f4: 910003e1     	mov	x1, sp
 21b73f8: 940d616e     	bl	0x250f9b0 <__RINvXs0_NtNtNtCslWxY2MhVcag_4core4iter8adapters3mapINtB6_3MapINtNtB8_9enumerate9EnumerateIBO_INtNtNtBc_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB23_11GroupRanges4iter0EENCINvNtNtNtB25_3fns6sum_v212dict_grouped6reducetNtB3E_14PreparedValuesKb1_E0ENtNtNtBa_6traits8iterator8Iterator4folduNCINvNvB4O_8for_each4callxNCINvMsa_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB61_9BufferMutxE11extend_iterBN_E0E0EB27_>
 21b73fc: ad4107e0     	ldp	q0, q1, [sp, #0x20]
 21b7400: ad010660     	stp	q0, q1, [x19, #0x20]
 21b7404: f94023e8     	ldr	x8, [sp, #0x40]
 21b7408: f9002268     	str	x8, [x19, #0x40]
 21b740c: ad4003e1     	ldp	q1, q0, [sp]
 21b7410: ad000261     	stp	q1, q0, [x19]
 21b7414: a94d7bfd     	ldp	x29, x30, [sp, #0xd0]
 21b7418: a94c4ff4     	ldp	x20, x19, [sp, #0xc0]
 21b741c: a94b57f6     	ldp	x22, x21, [sp, #0xb0]
 21b7420: a94a5ff8     	ldp	x24, x23, [sp, #0xa0]
 21b7424: 910383ff     	add	sp, sp, #0xe0
 21b7428: d65f03c0     	ret
 21b742c: d0036383     	adrp	x3, 0x8e29000 <_anon.0af241829979b29bb326c24b7616c55e.17+0x40>
 21b7430: 9102c063     	add	x3, x3, #0xb0
 21b7434: 953cd314     	bl	0x70ec084 <__RNvNtNtCslWxY2MhVcag_4core5slice5index16slice_index_fail>
 21b7438: 14000017     	b	0x21b7494 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x2e4>
 21b743c: f0028020     	adrp	x0, 0x71be000 <_anon.8eb0b0e7e102cb6a9875d7c3572da0cf.287+0x61>
 21b7440: 9128d400     	add	x0, x0, #0xa35
 21b7444: 90036402     	adrp	x2, 0x8e37000 <_anon.609ba3725e5fdd621b553d88f43057c6.5+0x108>
 21b7448: 91304042     	add	x2, x2, #0xc10
 21b744c: 52800661     	mov	w1, #0x33               ; =51
 21b7450: 953cd303     	bl	0x70ec05c <__RNvNtCslWxY2MhVcag_4core9panicking9panic_fmt>
 21b7454: 14000010     	b	0x21b7494 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x2e4>
 21b7458: 90027fc0     	adrp	x0, 0x71af000 <_anon.f6878b106fa7f3456c06c442e3dedb68.570+0x81>
 21b745c: 91224400     	add	x0, x0, #0x891
 21b7460: d0036382     	adrp	x2, 0x8e29000 <_anon.0af241829979b29bb326c24b7616c55e.17+0x40>
 21b7464: 91080042     	add	x2, x2, #0x200
 21b7468: 52800441     	mov	w1, #0x22               ; =34
 21b746c: 953cd2f7     	bl	0x70ec048 <__RNvNtCslWxY2MhVcag_4core9panicking5panic>
 21b7470: 14000009     	b	0x21b7494 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x2e4>
 21b7474: d0036382     	adrp	x2, 0x8e29000 <_anon.0af241829979b29bb326c24b7616c55e.17+0x40>
 21b7478: 910d2042     	add	x2, x2, #0x348
 21b747c: 953cd29c     	bl	0x70ebeec <__RNvNtCslWxY2MhVcag_4core9panicking18panic_bounds_check>
 21b7480: 14000005     	b	0x21b7494 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x2e4>
 21b7484: 90036402     	adrp	x2, 0x8e37000 <_anon.609ba3725e5fdd621b553d88f43057c6.5+0x108>
 21b7488: 912fe042     	add	x2, x2, #0xbf8
 21b748c: aa1403e0     	mov	x0, x20
 21b7490: 953cd297     	bl	0x70ebeec <__RNvNtCslWxY2MhVcag_4core9panicking18panic_bounds_check>
 21b7494: d4200020     	brk	#0x1
 21b7498: 14000001     	b	0x21b749c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetNtB5t_14PreparedValuesKb1_E0EEB3W_+0x2ec>
 21b749c: aa0003f3     	mov	x19, x0
 21b74a0: 910003e0     	mov	x0, sp
 21b74a4: 97ff2b3b     	bl	0x2182190 <__RINvNtCslWxY2MhVcag_4core3ptr9drop_glueINtNtCsattOkpHIiPg_13vortex_buffer10buffer_mut9BufferMutyEECs8Gx6krhMvpD_12vortex_array>
 21b74a8: aa1303e0     	mov	x0, x19
 21b74ac: 953cd480     	bl	0x70ec6ac <dyld_stub_binder+0x70ec6ac>
 21b74b0: 953cd289     	bl	0x70ebed4 <__RNvNtCslWxY2MhVcag_4core9panicking16panic_in_cleanup>
