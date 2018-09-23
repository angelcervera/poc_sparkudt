package org.apache.spark.custom.udts // we're calling some private API so need to be under 'org.apache.spark'

import java.io._

import com.acervera.poc.sparkcircularreference.Branch
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.sql.types.{DataType, UDTRegistration, UserDefinedType}



class BranchUDType extends UserDefinedType[Branch] {
  val kryo = new Kryo()

  override def sqlType: DataType = org.apache.spark.sql.types.BinaryType
  override def serialize(obj: Branch): Any = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(obj)
    bos.toByteArray
  }
  override def deserialize(datum: Any): Branch = {
    val bis = new ByteArrayInputStream(datum.asInstanceOf[Array[Byte]])
    val ois = new ObjectInputStream(bis)
    val obj = ois.readObject()
    obj.asInstanceOf[Branch]
  }

  override def userClass: Class[Branch] = classOf[Branch]
}

object BranchUDType {
  def register() = UDTRegistration.register(classOf[Branch].getName, classOf[BranchUDType].getName)
}
