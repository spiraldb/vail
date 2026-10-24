
/Users/danielking/.codex/worktrees/encoded-list-reduction/vail/vail/python/vail/_native.abi3.so:	file format mach-o arm64

Disassembly of section __TEXT,__text:

000000000219dcd0 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_>:
 219dcd0: d104c3ff     	sub	sp, sp, #0x130
 219dcd4: a90d6ffc     	stp	x28, x27, [sp, #0xd0]
 219dcd8: a90e67fa     	stp	x26, x25, [sp, #0xe0]
 219dcdc: a90f5ff8     	stp	x24, x23, [sp, #0xf0]
 219dce0: a91057f6     	stp	x22, x21, [sp, #0x100]
 219dce4: a9114ff4     	stp	x20, x19, [sp, #0x110]
 219dce8: a9127bfd     	stp	x29, x30, [sp, #0x120]
 219dcec: 910483fd     	add	x29, sp, #0x120
 219dcf0: aa0803f3     	mov	x19, x8
 219dcf4: 910183e8     	add	x8, sp, #0x60
 219dcf8: a9405016     	ldp	x22, x20, [x0]
 219dcfc: f9400817     	ldr	x23, [x0, #0x10]
 219dd00: 3cc28000     	ldur	q0, [x0, #0x28]
 219dd04: 3c8883e0     	stur	q0, [sp, #0x88]
 219dd08: 3cc38000     	ldur	q0, [x0, #0x38]
 219dd0c: 3c838100     	stur	q0, [x8, #0x38]
 219dd10: 3cc48000     	ldur	q0, [x0, #0x48]
 219dd14: 3c848100     	stur	q0, [x8, #0x48]
 219dd18: 3cc58000     	ldur	q0, [x0, #0x58]
 219dd1c: 3c858100     	stur	q0, [x8, #0x58]
 219dd20: 3cc18000     	ldur	q0, [x0, #0x18]
 219dd24: 3c8783e0     	stur	q0, [sp, #0x78]
 219dd28: eb1402e8     	subs	x8, x23, x20
 219dd2c: 9a8833f5     	csel	x21, xzr, x8, lo
 219dd30: 910063e8     	add	x8, sp, #0x18
 219dd34: aa1503e0     	mov	x0, x21
 219dd38: 52800061     	mov	w1, #0x3                ; =3
 219dd3c: 52800022     	mov	w2, #0x1                ; =1
 219dd40: 52800103     	mov	w3, #0x8                ; =8
 219dd44: d2800004     	mov	x4, #0x0                ; =0
 219dd48: 94004c25     	bl	0x21b0ddc <__RNvMs0_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB5_9BufferMutyE34with_capacity_preferred_aligned_inCs8Gx6krhMvpD_12vortex_array>
 219dd4c: a90653f6     	stp	x22, x20, [sp, #0x60]
 219dd50: f9003bf7     	str	x23, [sp, #0x70]
 219dd54: a944afe8     	ldp	x8, x11, [sp, #0x48]
 219dd58: cb08016a     	sub	x10, x11, x8
 219dd5c: eb0a02bf     	cmp	x21, x10
 219dd60: 540000c9     	b.ls	0x219dd78 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0xa8>
 219dd64: 910063e0     	add	x0, sp, #0x18
 219dd68: aa1503e1     	mov	x1, x21
 219dd6c: 9400481b     	bl	0x21afdd8 <__RNvMs0_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB5_9BufferMutyE16reserve_allocateCs8Gx6krhMvpD_12vortex_array>
 219dd70: a944afe8     	ldp	x8, x11, [sp, #0x48]
 219dd74: cb08016a     	sub	x10, x11, x8
 219dd78: f94023e9     	ldr	x9, [sp, #0x40]
 219dd7c: 8b080d29     	add	x9, x9, x8, lsl #3
 219dd80: eb08017f     	cmp	x11, x8
 219dd84: 54000061     	b.ne	0x219dd90 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0xc0>
 219dd88: aa0903ec     	mov	x12, x9
 219dd8c: 140000b5     	b	0x219e060 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x390>
 219dd90: f9403feb     	ldr	x11, [sp, #0x78]
 219dd94: eb1402ff     	cmp	x23, x20
 219dd98: 540015e9     	b.ls	0x219e054 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x384>
 219dd9c: a9480bed     	ldp	x13, x2, [sp, #0x80]
 219dda0: a949c3e8     	ldp	x8, x16, [sp, #0x98]
 219dda4: f9000be8     	str	x8, [sp, #0x10]
 219dda8: f9404be8     	ldr	x8, [sp, #0x90]
 219ddac: f90007e8     	str	x8, [sp, #0x8]
 219ddb0: a94a8ff1     	ldp	x17, x3, [sp, #0xa8]
 219ddb4: a94b9be1     	ldp	x1, x6, [sp, #0xb8]
 219ddb8: 52800025     	mov	w5, #0x1                ; =1
 219ddbc: aa0903ec     	mov	x12, x9
 219ddc0: f94002c8     	ldr	x8, [x22]
 219ddc4: b100051f     	cmn	x8, #0x1
 219ddc8: 54000140     	b.eq	0x219ddf0 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x120>
 219ddcc: f9400ac8     	ldr	x8, [x22, #0x10]
 219ddd0: eb08029f     	cmp	x20, x8
 219ddd4: 54001a42     	b.hs	0x219e11c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x44c>
 219ddd8: f94006c8     	ldr	x8, [x22, #0x8]
 219dddc: 8b141100     	add	x0, x8, x20, lsl #4
 219dde0: a9401c08     	ldp	x8, x7, [x0]
 219dde4: f94001a0     	ldr	x0, [x13]
 219dde8: b5000120     	cbnz	x0, 0x219de0c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x13c>
 219ddec: 14000016     	b	0x219de44 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x174>
 219ddf0: f94006c8     	ldr	x8, [x22, #0x8]
 219ddf4: eb08029f     	cmp	x20, x8
 219ddf8: 540017a2     	b.hs	0x219e0ec <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x41c>
 219ddfc: f9400ac7     	ldr	x7, [x22, #0x10]
 219de00: 9b147ce8     	mul	x8, x7, x20
 219de04: f94001a0     	ldr	x0, [x13]
 219de08: b40001e0     	cbz	x0, 0x219de44 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x174>
 219de0c: f100041f     	cmp	x0, #0x1
 219de10: 54000520     	b.eq	0x219deb4 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x1e4>
 219de14: f94005a0     	ldr	x0, [x13, #0x8]
 219de18: f9401c04     	ldr	x4, [x0, #0x38]
 219de1c: eb04017f     	cmp	x11, x4
 219de20: 540014c2     	b.hs	0x219e0b8 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x3e8>
 219de24: f9400804     	ldr	x4, [x0, #0x10]
 219de28: f9401800     	ldr	x0, [x0, #0x30]
 219de2c: 8b0b0000     	add	x0, x0, x11
 219de30: d343fc17     	lsr	x23, x0, #3
 219de34: 38776884     	ldrb	w4, [x4, x23]
 219de38: 92400800     	and	x0, x0, #0x7
 219de3c: 1ac02480     	lsr	w0, w4, w0
 219de40: 360003a0     	tbz	w0, #0x0, 0x219deb4 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x1e4>
 219de44: b4000e07     	cbz	x7, 0x219e004 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x334>
 219de48: f9400040     	ldr	x0, [x2]
 219de4c: cb000100     	sub	x0, x8, x0
 219de50: ab070008     	adds	x8, x0, x7
 219de54: 54001402     	b.hs	0x219e0d4 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x404>
 219de58: f9400bee     	ldr	x14, [sp, #0x10]
 219de5c: eb0e011f     	cmp	x8, x14
 219de60: 540013a8     	b.hi	0x219e0d4 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x404>
 219de64: f9400208     	ldr	x8, [x16]
 219de68: f100051f     	cmp	x8, #0x1
 219de6c: 54000280     	b.eq	0x219debc <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x1ec>
 219de70: f94007ee     	ldr	x14, [sp, #0x8]
 219de74: 8b0005d7     	add	x23, x14, x0, lsl #1
 219de78: d37ff8f9     	lsl	x25, x7, #1
 219de7c: 8b1902f8     	add	x24, x23, x25
 219de80: b5000228     	cbnz	x8, 0x219dec4 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x1f4>
 219de84: f9400227     	ldr	x7, [x17]
 219de88: f10004ff     	cmp	x7, #0x1
 219de8c: 54000180     	b.eq	0x219debc <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x1ec>
 219de90: b50007e7     	cbnz	x7, 0x219df8c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x2bc>
 219de94: 784026e8     	ldrh	w8, [x23], #0x2
 219de98: eb08003f     	cmp	x1, x8
 219de9c: 54001369     	b.ls	0x219e108 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x438>
 219dea0: b8a87868     	ldrsw	x8, [x3, x8, lsl #2]
 219dea4: 8b0800e7     	add	x7, x7, x8
 219dea8: f1000b39     	subs	x25, x25, #0x2
 219deac: 54ffff41     	b.ne	0x219de94 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x1c4>
 219deb0: 1400005f     	b	0x219e02c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x35c>
 219deb4: d2800007     	mov	x7, #0x0                ; =0
 219deb8: 1400005d     	b	0x219e02c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x35c>
 219debc: d2800007     	mov	x7, #0x0                ; =0
 219dec0: 14000051     	b	0x219e004 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x334>
 219dec4: d2800007     	mov	x7, #0x0                ; =0
 219dec8: 52800019     	mov	w25, #0x0               ; =0
 219decc: d280001a     	mov	x26, #0x0               ; =0
 219ded0: f940061b     	ldr	x27, [x16, #0x8]
 219ded4: f9401f7c     	ldr	x28, [x27, #0x38]
 219ded8: 8b1a001e     	add	x30, x0, x26
 219dedc: 9100075a     	add	x26, x26, #0x1
 219dee0: 14000006     	b	0x219def8 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x228>
 219dee4: 91000af7     	add	x23, x23, #0x2
 219dee8: 910007de     	add	x30, x30, #0x1
 219deec: 9100075a     	add	x26, x26, #0x1
 219def0: eb1802ff     	cmp	x23, x24
 219def4: 54000860     	b.eq	0x219e000 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x330>
 219def8: eb1c03df     	cmp	x30, x28
 219defc: 54000de2     	b.hs	0x219e0b8 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x3e8>
 219df00: f9400b68     	ldr	x8, [x27, #0x10]
 219df04: f9401b64     	ldr	x4, [x27, #0x30]
 219df08: 8b0403c4     	add	x4, x30, x4
 219df0c: d343fc8f     	lsr	x15, x4, #3
 219df10: 386f6908     	ldrb	w8, [x8, x15]
 219df14: 9240088f     	and	x15, x4, #0x7
 219df18: 1acf2508     	lsr	w8, w8, w15
 219df1c: 3607fe48     	tbz	w8, #0x0, 0x219dee4 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x214>
 219df20: f9400224     	ldr	x4, [x17]
 219df24: f100049f     	cmp	x4, #0x1
 219df28: 54fffde0     	b.eq	0x219dee4 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x214>
 219df2c: 794002e8     	ldrh	w8, [x23]
 219df30: f100089f     	cmp	x4, #0x2
 219df34: 540001a1     	b.ne	0x219df68 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x298>
 219df38: f9400624     	ldr	x4, [x17, #0x8]
 219df3c: f9401c8f     	ldr	x15, [x4, #0x38]
 219df40: eb0801ff     	cmp	x15, x8
 219df44: 54000ba9     	b.ls	0x219e0b8 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x3e8>
 219df48: f940088f     	ldr	x15, [x4, #0x10]
 219df4c: f9401884     	ldr	x4, [x4, #0x30]
 219df50: 8b080084     	add	x4, x4, x8
 219df54: d343fc8e     	lsr	x14, x4, #3
 219df58: 386e69ee     	ldrb	w14, [x15, x14]
 219df5c: 9240088f     	and	x15, x4, #0x7
 219df60: 1acf25ce     	lsr	w14, w14, w15
 219df64: 3607fc0e     	tbz	w14, #0x0, 0x219dee4 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x214>
 219df68: eb08003f     	cmp	x1, x8
 219df6c: 54000ce9     	b.ls	0x219e108 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x438>
 219df70: 91000af7     	add	x23, x23, #0x2
 219df74: b8a87868     	ldrsw	x8, [x3, x8, lsl #2]
 219df78: 8b0800e7     	add	x7, x7, x8
 219df7c: 52800039     	mov	w25, #0x1               ; =1
 219df80: eb1802ff     	cmp	x23, x24
 219df84: 54fffaa1     	b.ne	0x219ded8 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x208>
 219df88: 14000029     	b	0x219e02c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x35c>
 219df8c: d2800007     	mov	x7, #0x0                ; =0
 219df90: 52800019     	mov	w25, #0x0               ; =0
 219df94: f9400620     	ldr	x0, [x17, #0x8]
 219df98: f9401c1a     	ldr	x26, [x0, #0x38]
 219df9c: aa1703fb     	mov	x27, x23
 219dfa0: 78402768     	ldrh	w8, [x27], #0x2
 219dfa4: eb08035f     	cmp	x26, x8
 219dfa8: 54000889     	b.ls	0x219e0b8 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x3e8>
 219dfac: f9400804     	ldr	x4, [x0, #0x10]
 219dfb0: f940181c     	ldr	x28, [x0, #0x30]
 219dfb4: 8b08039c     	add	x28, x28, x8
 219dfb8: d343ff9e     	lsr	x30, x28, #3
 219dfbc: 387e6884     	ldrb	w4, [x4, x30]
 219dfc0: 92400b9c     	and	x28, x28, #0x7
 219dfc4: 1adc2484     	lsr	w4, w4, w28
 219dfc8: 370000a4     	tbnz	w4, #0x0, 0x219dfdc <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x30c>
 219dfcc: aa1b03f7     	mov	x23, x27
 219dfd0: eb18037f     	cmp	x27, x24
 219dfd4: 54fffe61     	b.ne	0x219dfa0 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x2d0>
 219dfd8: 1400000a     	b	0x219e000 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x330>
 219dfdc: eb08003f     	cmp	x1, x8
 219dfe0: 54000949     	b.ls	0x219e108 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x438>
 219dfe4: 91000af7     	add	x23, x23, #0x2
 219dfe8: b8a87868     	ldrsw	x8, [x3, x8, lsl #2]
 219dfec: 8b0800e7     	add	x7, x7, x8
 219dff0: 52800039     	mov	w25, #0x1               ; =1
 219dff4: eb1802ff     	cmp	x23, x24
 219dff8: 54fffd21     	b.ne	0x219df9c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x2cc>
 219dffc: 1400000c     	b	0x219e02c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x35c>
 219e000: 37000179     	tbnz	w25, #0x0, 0x219e02c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x35c>
 219e004: f94014c8     	ldr	x8, [x6, #0x28]
 219e008: f94024cf     	ldr	x15, [x6, #0x48]
 219e00c: 8b0b01ef     	add	x15, x15, x11
 219e010: 924009e0     	and	x0, x15, #0x7
 219e014: 5280002e     	mov	w14, #0x1               ; =1
 219e018: 1ac021c0     	lsl	w0, w14, w0
 219e01c: d343fdef     	lsr	x15, x15, #3
 219e020: 386f6904     	ldrb	w4, [x8, x15]
 219e024: 2a040000     	orr	w0, w0, w4
 219e028: 382f6900     	strb	w0, [x8, x15]
 219e02c: 91000694     	add	x20, x20, #0x1
 219e030: 9100056b     	add	x11, x11, #0x1
 219e034: f8008587     	str	x7, [x12], #0x8
 219e038: eb0a00bf     	cmp	x5, x10
 219e03c: 54000080     	b.eq	0x219e04c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x37c>
 219e040: eb1500bf     	cmp	x5, x21
 219e044: 910004a5     	add	x5, x5, #0x1
 219e048: 54ffebc1     	b.ne	0x219ddc0 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0xf0>
 219e04c: f94027e8     	ldr	x8, [sp, #0x48]
 219e050: 14000002     	b	0x219e058 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x388>
 219e054: aa0903ec     	mov	x12, x9
 219e058: f90037f4     	str	x20, [sp, #0x68]
 219e05c: f9003feb     	str	x11, [sp, #0x78]
 219e060: cb090189     	sub	x9, x12, x9
 219e064: 8b490d08     	add	x8, x8, x9, lsr #3
 219e068: f90027e8     	str	x8, [sp, #0x48]
 219e06c: 910183e0     	add	x0, sp, #0x60
 219e070: 910063e1     	add	x1, sp, #0x18
 219e074: 940d33c6     	bl	0x24eaf8c <__RINvXs0_NtNtNtCslWxY2MhVcag_4core4iter8adapters3mapINtB6_3MapINtNtB8_9enumerate9EnumerateIBO_INtNtNtBc_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB23_11GroupRanges4iter0EENCINvNtNtNtB25_3fns6sum_v212dict_grouped6reducetKb0_KB4m_E0ENtNtNtBa_6traits8iterator8Iterator4folduNCINvNvB4x_8for_each4callxNCINvMsa_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB5K_9BufferMutxE11extend_iterBN_E0E0EB27_>
 219e078: 3cc383e0     	ldur	q0, [sp, #0x38]
 219e07c: 3cc483e1     	ldur	q1, [sp, #0x48]
 219e080: ad010660     	stp	q0, q1, [x19, #0x20]
 219e084: f9402fe8     	ldr	x8, [sp, #0x58]
 219e088: f9002268     	str	x8, [x19, #0x40]
 219e08c: 3cc283e0     	ldur	q0, [sp, #0x28]
 219e090: 3cc183e1     	ldur	q1, [sp, #0x18]
 219e094: ad000261     	stp	q1, q0, [x19]
 219e098: a9527bfd     	ldp	x29, x30, [sp, #0x120]
 219e09c: a9514ff4     	ldp	x20, x19, [sp, #0x110]
 219e0a0: a95057f6     	ldp	x22, x21, [sp, #0x100]
 219e0a4: a94f5ff8     	ldp	x24, x23, [sp, #0xf0]
 219e0a8: a94e67fa     	ldp	x26, x25, [sp, #0xe0]
 219e0ac: a94d6ffc     	ldp	x28, x27, [sp, #0xd0]
 219e0b0: 9104c3ff     	add	sp, sp, #0x130
 219e0b4: d65f03c0     	ret
 219e0b8: 90027fe0     	adrp	x0, 0x719a000 <_anon.7227fbafd4c2d33105ad0fdc98171d5d.76+0x1887>
 219e0bc: 910a7c00     	add	x0, x0, #0x29f
 219e0c0: b0036422     	adrp	x2, 0x8e23000 <_anon.c66d5325494e41c4dd97521f54759f7e.47+0x20>
 219e0c4: 91132042     	add	x2, x2, #0x4c8
 219e0c8: 52800441     	mov	w1, #0x22               ; =34
 219e0cc: 953d2353     	bl	0x70e6e18 <__RNvNtCslWxY2MhVcag_4core9panicking5panic>
 219e0d0: 14000018     	b	0x219e130 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x460>
 219e0d4: b0036423     	adrp	x3, 0x8e23000 <_anon.c66d5325494e41c4dd97521f54759f7e.47+0x20>
 219e0d8: 910b2063     	add	x3, x3, #0x2c8
 219e0dc: aa0803e1     	mov	x1, x8
 219e0e0: f9400be2     	ldr	x2, [sp, #0x10]
 219e0e4: 953d235c     	bl	0x70e6e54 <__RNvNtNtCslWxY2MhVcag_4core5slice5index16slice_index_fail>
 219e0e8: 14000012     	b	0x219e130 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x460>
 219e0ec: b00280c0     	adrp	x0, 0x71b7000 <_anon.16a87c550748bb403c38fba30592ac60.822+0x108>
 219e0f0: 912b7800     	add	x0, x0, #0xade
 219e0f4: b0036482     	adrp	x2, 0x8e2f000 <_anon.165ed89ec24b51727a4f07f818d28b29.61+0x830>
 219e0f8: 91086042     	add	x2, x2, #0x218
 219e0fc: 52800661     	mov	w1, #0x33               ; =51
 219e100: 953d234b     	bl	0x70e6e2c <__RNvNtCslWxY2MhVcag_4core9panicking9panic_fmt>
 219e104: 1400000b     	b	0x219e130 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x460>
 219e108: b0036422     	adrp	x2, 0x8e23000 <_anon.c66d5325494e41c4dd97521f54759f7e.47+0x20>
 219e10c: 910ac042     	add	x2, x2, #0x2b0
 219e110: aa0803e0     	mov	x0, x8
 219e114: 953d22ea     	bl	0x70e6cbc <__RNvNtCslWxY2MhVcag_4core9panicking18panic_bounds_check>
 219e118: 14000006     	b	0x219e130 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x460>
 219e11c: b0036482     	adrp	x2, 0x8e2f000 <_anon.165ed89ec24b51727a4f07f818d28b29.61+0x830>
 219e120: 91080042     	add	x2, x2, #0x200
 219e124: aa1403e0     	mov	x0, x20
 219e128: aa0803e1     	mov	x1, x8
 219e12c: 953d22e4     	bl	0x70e6cbc <__RNvNtCslWxY2MhVcag_4core9panicking18panic_bounds_check>
 219e130: d4200020     	brk	#0x1
 219e134: 14000001     	b	0x219e138 <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x468>
 219e138: aa0003f3     	mov	x19, x0
 219e13c: 910063f4     	add	x20, sp, #0x18
 219e140: 910063e0     	add	x0, sp, #0x18
 219e144: 941c14cd     	bl	0x28a3478 <__RNvXs7_NtCsattOkpHIiPg_13vortex_buffer10allocationNtB5_10AllocationNtNtNtCslWxY2MhVcag_4core3ops4drop4Drop4drop>
 219e148: f9401be8     	ldr	x8, [sp, #0x30]
 219e14c: b4000108     	cbz	x8, 0x219e16c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x49c>
 219e150: 92800009     	mov	x9, #-0x1               ; =-1
 219e154: f8690108     	ldaddl	x9, x8, [x8]
 219e158: f100051f     	cmp	x8, #0x1
 219e15c: 54000081     	b.ne	0x219e16c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x49c>
 219e160: d50339bf     	dmb	ishld
 219e164: 91006280     	add	x0, x20, #0x18
 219e168: 941bff48     	bl	0x289de88 <__RNvMsn_NtCs6KVRSXc8uZF_5alloc4syncINtB5_3ArcDNtNtCsattOkpHIiPg_13vortex_buffer10allocation15BufferAllocatorEL_E9drop_slowBL_>
 219e16c: aa1303e0     	mov	x0, x19
 219e170: 953d24c3     	bl	0x70e747c <dyld_stub_binder+0x70e747c>
 219e174: 953d22cc     	bl	0x70e6ca4 <__RNvNtCslWxY2MhVcag_4core9panicking16panic_in_cleanup>
 219e178: f9401be8     	ldr	x8, [sp, #0x30]
 219e17c: b4000108     	cbz	x8, 0x219e19c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x4cc>
 219e180: 92800009     	mov	x9, #-0x1               ; =-1
 219e184: f8690108     	ldaddl	x9, x8, [x8]
 219e188: f100051f     	cmp	x8, #0x1
 219e18c: 54000081     	b.ne	0x219e19c <__RINvXsd_NtCsattOkpHIiPg_13vortex_buffer10buffer_mutINtB6_9BufferMutxEINtNtNtNtCslWxY2MhVcag_4core4iter6traits7collect12FromIteratorxE9from_iterINtNtNtB1c_8adapters3map3MapINtNtB2m_9enumerate9EnumerateIB2i_INtNtNtB1e_3ops5range5RangejENCNvMs1_NtNtCs8Gx6krhMvpD_12vortex_array12aggregate_fn19accumulator_groupedNtB3S_11GroupRanges4iter0EENCINvNtNtNtB3U_3fns6sum_v212dict_grouped6reducetKb0_KB6b_E0EEB3W_+0x4cc>
 219e190: d50339bf     	dmb	ishld
 219e194: 91006280     	add	x0, x20, #0x18
 219e198: 941bff3c     	bl	0x289de88 <__RNvMsn_NtCs6KVRSXc8uZF_5alloc4syncINtB5_3ArcDNtNtCsattOkpHIiPg_13vortex_buffer10allocation15BufferAllocatorEL_E9drop_slowBL_>
 219e19c: 953d22c2     	bl	0x70e6ca4 <__RNvNtCslWxY2MhVcag_4core9panicking16panic_in_cleanup>
 219e1a0: 953d22c1     	bl	0x70e6ca4 <__RNvNtCslWxY2MhVcag_4core9panicking16panic_in_cleanup>
