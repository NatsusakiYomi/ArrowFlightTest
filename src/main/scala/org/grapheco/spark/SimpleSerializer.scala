package org.grapheco.spark

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 18:28
 * @Modified By:
 */
import java.io._

object SimpleSerializer {

  def serialize(obj: Serializable): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    try {
      oos.writeObject(obj)
      oos.flush()
      baos.toByteArray
    } finally {
      oos.close()
      baos.close()
    }
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    try {
      ois.readObject().asInstanceOf[T]
    } finally {
      ois.close()
      bais.close()
    }
  }
}

