#include <unistd.h>
#include <stdio.h>
#include <stdatomic.h>
#include <sys/types.h>
#include <sys/stat.h>
static _Atomic unsigned long long counts[8], bytes[8];
static ssize_t counted_write(int fd, const void *buf, size_t n) {
    struct stat st;
    if(fd>2 && fstat(fd,&st)==0 && S_ISREG(st.st_mode)){ unsigned b=n<64?0:n<1024?1:n<16384?2:n<65536?3:n<262144?4:n<1048576?5:n<4194304?6:7; atomic_fetch_add(&counts[b],1); atomic_fetch_add(&bytes[b],n); }
    return write(fd,buf,n);
}
__attribute__((used)) static struct {const void *replacement; const void *replacee;} interposers[] __attribute__((section("__DATA,__interpose"))) = {{(const void*)counted_write,(const void*)write}};
__attribute__((destructor)) static void report(void){ for(unsigned i=0;i<8;i++)fprintf(stderr,"WRITE_BUCKET %u %llu %llu\n",i,atomic_load(&counts[i]),atomic_load(&bytes[i])); }
