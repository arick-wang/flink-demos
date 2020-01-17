package com.znv

object StringUtil {

  def reverse(str: String): String = {
    str match {
      case s if s!=null => s.reverse
      case _ => ""
    }

  }

}
