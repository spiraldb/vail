use std::{hint::black_box,time::Instant};
#[derive(Clone,Copy)] struct Bits<'a>{data:&'a[u8],offset:usize}
impl Bits<'_>{#[inline(always)] fn word(self,index:usize,n:usize)->u64{let bit=index+self.offset;let at=bit/8;let shift=bit%8;let mut b=[0u8;8];let take=8.min(self.data.len()-at);b[..take].copy_from_slice(&self.data[at..at+take]);let mut v=u64::from_le_bytes(b)>>shift;if shift!=0&&at+8<self.data.len(){v|=(self.data[at+8]as u64)<<(64-shift);}if n!=64{v&=(1u64<<n)-1;}v}}
#[inline(always)]fn add_word(out:&mut[u64],mut word:u64,weight:u64){if word==u64::MAX&&out.len()==64{for v in out{*v=v.wrapping_add(weight);}return;}while word!=0{let i=word.trailing_zeros()as usize;out[i]=out[i].wrapping_add(weight);word&=word-1;}}
fn direct(bits:Bits,width:usize,rows:usize,out:&mut[u64]){for r in 0..rows{for c in (0..width).step_by(64){let n=(width-c).min(64);add_word(&mut out[c..c+n],bits.word(r*width+c,n),1);}}}
fn scalar(bits:Bits,width:usize,rows:usize,out:&mut[u64]){for r in 0..rows{for c in (0..width).step_by(64){let n=(width-c).min(64);let mut w=bits.word(r*width+c,n);for v in &mut out[c..c+n]{*v+=w&1;w>>=1;}}}}
#[inline(always)]fn csa(a:u64,b:u64,c:u64)->(u64,u64){let x=a^b;((a&b)|(x&c),x^c)}
fn carry(bits:Bits,width:usize,rows:usize,out:&mut[u64]){for col in (0..width).step_by(64){let n=(width-col).min(64);let dst=&mut out[col..col+n];let(mut one,mut two,mut four,mut eight)=(0,0,0,0);let mut r=0;while r+16<=rows{let mut w=[0u64;16];for(i,v)in w.iter_mut().enumerate(){*v=bits.word((r+i)*width+col,n);}
let(a,x)=csa(one,w[0],w[1]);let(b,y)=csa(x,w[2],w[3]);one=y;let(c,x)=csa(two,a,b);two=x;
let(a,x)=csa(one,w[4],w[5]);let(b,y)=csa(x,w[6],w[7]);one=y;let(d,x)=csa(two,a,b);two=x;let(e,x)=csa(four,c,d);four=x;
let(a,x)=csa(one,w[8],w[9]);let(b,y)=csa(x,w[10],w[11]);one=y;let(c,x)=csa(two,a,b);two=x;
let(a,x)=csa(one,w[12],w[13]);let(b,y)=csa(x,w[14],w[15]);one=y;let(d,x)=csa(two,a,b);two=x;let(f,x)=csa(four,c,d);four=x;
let(sixteen,x)=csa(eight,e,f);eight=x;add_word(dst,sixteen,16);r+=16;}
for rr in r..rows{add_word(dst,bits.word(rr*width+col,n),1);}for(w,k)in[(one,1),(two,2),(four,4),(eight,8)]{add_word(dst,w,k);}}}
#[cfg(target_arch="aarch64")]
fn neon(bits:Bits,width:usize,rows:usize,out:&mut[u64]){use std::arch::aarch64::*;unsafe{let lookup=[0u8,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1];let masks=[1u8,2,4,8,16,32,64,128,1,2,4,8,16,32,64,128];let lookup=vld1q_u8(lookup.as_ptr());let masks=vld1q_u8(masks.as_ptr());for col in (0..width).step_by(64){let n=(width-col).min(64);for start in (0..rows).step_by(255){let mut acc=[vdupq_n_u8(0);4];for r in start..(start+255).min(rows){let w=bits.word(r*width+col,n);for(i,a)in acc.iter_mut().enumerate(){let v=vreinterpretq_u8_u16(vdupq_n_u16((w>>(16*i))as u16));let v=vqtbl1q_u8(v,lookup);*a=vsubq_u8(*a,vtstq_u8(v,masks));}}let mut count=[0u8;64];for(i,a)in acc.iter().enumerate(){vst1q_u8(count.as_mut_ptr().add(i*16),*a);}for(v,c)in out[col..col+n].iter_mut().zip(count){*v+=c as u64;}}}}}
#[cfg(target_arch="aarch64")]
fn neon_skip(bits:Bits,width:usize,rows:usize,out:&mut[u64]){use std::arch::aarch64::*;unsafe{let lookup=[0u8,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1];let masks=[1u8,2,4,8,16,32,64,128,1,2,4,8,16,32,64,128];let lookup=vld1q_u8(lookup.as_ptr());let masks=vld1q_u8(masks.as_ptr());for col in (0..width).step_by(64){let n=(width-col).min(64);for start in (0..rows).step_by(255){let mut acc=[vdupq_n_u8(0);4];for r in start..(start+255).min(rows){let w=bits.word(r*width+col,n);if w==0{continue;}for(i,a)in acc.iter_mut().enumerate(){let v=vreinterpretq_u8_u16(vdupq_n_u16((w>>(16*i))as u16));let v=vqtbl1q_u8(v,lookup);*a=vsubq_u8(*a,vtstq_u8(v,masks));}}let mut count=[0u8;64];for(i,a)in acc.iter().enumerate(){vst1q_u8(count.as_mut_ptr().add(i*16),*a);}for(v,c)in out[col..col+n].iter_mut().zip(count){*v+=c as u64;}}}}}
#[cfg(target_arch="aarch64")]
fn neon_csa(bits:Bits,width:usize,rows:usize,out:&mut[u64]){use std::arch::aarch64::*;unsafe{
let idx=[0u8,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1];let mask=[1u8,2,4,8,16,32,64,128,1,2,4,8,16,32,64,128];let idx=vld1q_u8(idx.as_ptr());let mask=vld1q_u8(mask.as_ptr());
for col in(0..width).step_by(64){let n=(width-col).min(64);for start in(0..rows).step_by(255){let mut acc=[vdupq_n_u8(0);4];let end=(start+255).min(rows);let mut r=start;
while r+15<=end{
let w=|i|bits.word((r+i)*width+col,n);
let(a0,b0)=csa(w(0),w(1),w(2));let(a1,b1)=csa(w(3),w(4),w(5));let(a2,b2)=csa(w(6),w(7),w(8));let(a3,b3)=csa(w(9),w(10),w(11));let(a4,b4)=csa(w(12),w(13),w(14));
let(a5,b5)=csa(b0,b1,b2);let(a6,one)=csa(b3,b4,b5);let(a7,b6)=csa(a0,a1,a2);let(a8,b7)=csa(a3,a4,a5);let(a9,two)=csa(b6,b7,a6);let(eight,four)=csa(a7,a8,a9);
for(i,a)in acc.iter_mut().enumerate(){let lane=|x:u64|vtstq_u8(vqtbl1q_u8(vreinterpretq_u8_u16(vdupq_n_u16((x>>(16*i))as u16)),idx),mask);
let x1=vandq_u8(lane(one),vdupq_n_u8(1));let x2=vandq_u8(lane(two),vdupq_n_u8(2));let x4=vandq_u8(lane(four),vdupq_n_u8(4));let x8=vandq_u8(lane(eight),vdupq_n_u8(8));*a=vaddq_u8(*a,vorrq_u8(vorrq_u8(x1,x2),vorrq_u8(x4,x8)));}r+=15;
}
for row in r..end{let word=bits.word(row*width+col,n);for(i,a)in acc.iter_mut().enumerate(){let v=vreinterpretq_u8_u16(vdupq_n_u16((word>>(16*i))as u16));*a=vsubq_u8(*a,vtstq_u8(vqtbl1q_u8(v,idx),mask));}}
let mut counts=[0u8;64];for(i,a)in acc.iter().enumerate(){vst1q_u8(counts.as_mut_ptr().add(16*i),*a);}for(v,c)in out[col..col+n].iter_mut().zip(counts){*v+=c as u64;}
}}}}
fn main(){for width in [1,7,31,32,63,64,65,1000,2535,50000]{for rows in [1,25,256,1024]{if width*rows>3000000{continue;}for density in [0u64,1,50,99,100]{let mut data=vec![0u8;(width*rows+7+7)/8];let mut seed=5u64;for i in 0..width*rows{seed^=seed<<13;seed^=seed>>7;seed^=seed<<17;if seed%100<density{let j=i+3;data[j/8]|=1<<(j%8);}}let bits=Bits{data:&data,offset:3};let mut oracle=vec![0; width];scalar(bits,width,rows,&mut oracle);let funcs:[(&str,fn(Bits,usize,usize,&mut[u64]));6]=[("scalar",scalar),("sparse",direct),("csa",carry),("neon",neon),("neon_skip",neon_skip),("neon_csa",neon_csa)];let mut times=vec![];for(name,f)in funcs{let mut out=vec![0;width];f(bits,width,rows,&mut out);assert_eq!(out,oracle,"{name} {width} {rows} {density}");let n=(1000000/(width*rows)).clamp(3,1000);let start=Instant::now();for _ in 0..n{f(black_box(bits),width,rows,black_box(&mut out));}times.push(format!("{}={:.3}",name,start.elapsed().as_nanos()as f64/(n*width*rows)as f64));}println!("w={} rows={} density={} {}",width,rows,density,times.join(" "));}}}}
