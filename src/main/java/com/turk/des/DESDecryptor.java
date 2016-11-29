package com.turk.des;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import sun.misc.BASE64Decoder;

public class DESDecryptor
{
  private Throwable exception;
  private static final byte[] IV = { 18, 52, 86, 120, -112, -85, -51, -17 };
  private static final BASE64Decoder BASE64_DECODER = new BASE64Decoder();
 
  public String desDecrypt(String input, String decryptKey)
  {
    if ((input == null) || (decryptKey == null) || (decryptKey.length() < 8)) return null;

    try
    {
      byte[] byteKey = decryptKey.substring(0, 8).getBytes("ASCII");

      DesPKCS7Encrypter encrypter = new DesPKCS7Encrypter(byteKey, IV);

      ByteArrayInputStream bais = new ByteArrayInputStream(BASE64_DECODER.decodeBuffer(input));

      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      encrypter.decrypt(bais, baos);

      String result = baos.toString("utf-8");
      this.exception = null;
      return result;
    }
    catch (Exception e)
    {
      this.exception = e;
    }return null;
  }

  public Throwable getLastException()
  {
    return this.exception;
  }

  public static void main(String[] args)
  {
    DESDecryptor des = new DESDecryptor();
    System.out.println(des.desDecrypt("YCQWxx09MAq/crZbLGBnEvpyc2kZwa4JcY5a+YMpkPvL49j4fwMgLA==", "UWAY@SOFT2009"));
  }
}