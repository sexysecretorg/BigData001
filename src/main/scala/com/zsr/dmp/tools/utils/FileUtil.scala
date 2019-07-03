package com.zsr.dmp.tools.utils

import java.io.File


object FileUtil {
  /**
    * 删除一个文件夹,及其子目录
    *
    * @param dir
    */
  def deleteDir(dir: File): Unit = {
    if(!dir.exists()){
      return
    }
    val files = dir.listFiles()
    files.foreach(f => {
      if (f.isDirectory) {
        deleteDir(f)
      } else {
        f.delete()
      }
    })
    dir.delete()
  }

}
