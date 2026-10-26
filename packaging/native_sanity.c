/* Native ABI/lifecycle sanity only. No database/model/session/inference. */
#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <gnu/libc-version.h>
#include <libavcodec/version.h>
#include <libavformat/version.h>
#include <libavutil/version.h>
#include <libswscale/version.h>
#include <libswresample/version.h>
#include <onnxruntime/onnxruntime_c_api.h>
_Static_assert(LIBAVCODEC_VERSION_INT==((63U<<16)|(1U<<8)|102U),"FFmpeg9 codec headers required");
_Static_assert(LIBAVFORMAT_VERSION_INT==((63U<<16)|(1U<<8)|102U),"FFmpeg9 format headers required");
_Static_assert(LIBAVUTIL_VERSION_INT==((61U<<16)|(1U<<8)|102U),"FFmpeg9 utility headers required");
_Static_assert(LIBSWSCALE_VERSION_INT==((10U<<16)|(1U<<8)|102U),"FFmpeg9 scaler headers required");
_Static_assert(LIBSWRESAMPLE_VERSION_INT==((7U<<16)|(1U<<8)|102U),"FFmpeg9 resampler headers required");
static void* library(const char* name){void* p=dlopen(name,RTLD_NOW|RTLD_LOCAL);if(!p){fprintf(stderr,"Native library unavailable: %s\n",name);exit(1);}return p;}
static void* symbol(void* handle,const char* name){void* p=dlsym(handle,name);if(!p){fprintf(stderr,"Native symbol unavailable: %s\n",name);exit(1);}return p;}
static void version(const char* lib,const char* name,uint32_t expected){void* p=library(lib);uint32_t(*f)(void)=symbol(p,name);if(f()!=expected){fprintf(stderr,"Native ABI version differs: %s\n",name);exit(1);}dlclose(p);}
int main(void){
 if(strcmp(gnu_get_libc_version(),"2.41")){fputs("Runtime glibc must be2.41\n",stderr);return 1;}
 version("libavcodec.so.63","avcodec_version",(63U<<16)|(1U<<8)|102U);
 version("libavformat.so.63","avformat_version",(63U<<16)|(1U<<8)|102U);
 version("libavutil.so.61","avutil_version",(61U<<16)|(1U<<8)|102U);
 version("libswscale.so.10","swscale_version",(10U<<16)|(1U<<8)|102U);
 version("libswresample.so.7","swresample_version",(7U<<16)|(1U<<8)|102U);
 void* avif=library("libavif.so.16");const char*(*avif_version)(void)=symbol(avif,"avifVersion");
 if(strcmp(avif_version(),"1.4.2"))return 1;
 void*(*avif_create)(void)=symbol(avif,"avifDecoderCreate");void(*avif_destroy)(void*)=symbol(avif,"avifDecoderDestroy");void* decoder=avif_create();if(!decoder)return 1;avif_destroy(decoder);dlclose(avif);
 void* heif=library("libheif.so.1");const char*(*heif_version)(void)=symbol(heif,"heif_get_version");if(strcmp(heif_version(),"1.23.6"))return 1;dlclose(heif);
 void* jpeg=library("libturbojpeg.so.0");void*(*jpeg_create)(int)=symbol(jpeg,"tj3Init");void(*jpeg_destroy)(void*)=symbol(jpeg,"tj3Destroy");void* compressor=jpeg_create(0);if(!compressor)return 1;jpeg_destroy(compressor);dlclose(jpeg);
 const char* pdfium_path=getenv("MEMENTO_PDFIUM_LIBRARY");if(!pdfium_path)return 1;
 void* pdfium=library(pdfium_path);void(*pdf_init)(void)=symbol(pdfium,"FPDF_InitLibrary");void(*pdf_drop)(void)=symbol(pdfium,"FPDF_DestroyLibrary");pdf_init();pdf_drop();dlclose(pdfium);
 const char* ort_path=getenv("MEMENTO_ORT_LIBRARY");if(!ort_path)return 1;
 void* ort=library(ort_path);const OrtApiBase*(*ort_base)(void)=symbol(ort,"OrtGetApiBase");const OrtApiBase* api=ort_base();if(!api||strcmp(api->GetVersionString(),"1.31.0")||!api->GetApi(18))return 1;dlclose(ort);
 puts("Native runtime ABI/lifecycle sanity passed; no inference performed.");return 0;
}
