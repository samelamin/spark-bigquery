package com.samelamin.spark.bigquery.utils

import java.util.{Collections, Map => JavaMap}
import collection.JavaConversions._
/**
  * Created by root on 2/13/17.
  */
object EnvHacker {
  def setEnv(newEnv: Map[String, String]): Unit = {
    try {
      val processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment")
      val theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment")
      theEnvironmentField.setAccessible(true)

      val variableClass = Class.forName("java.lang.ProcessEnvironment$Variable")
      val convertToVariable = variableClass.getMethod("valueOf", classOf[java.lang.String])
      convertToVariable.setAccessible(true)

      val valueClass = Class.forName("java.lang.ProcessEnvironment$Value")
      val convertToValue = valueClass.getMethod("valueOf", classOf[java.lang.String])
      convertToValue.setAccessible(true)

      val sampleVariable = convertToVariable.invoke(null, "")
      val sampleValue = convertToValue.invoke(null, "")
      val env = theEnvironmentField.get(null).asInstanceOf[JavaMap[sampleVariable.type, sampleValue.type]]
      newEnv.foreach { case (k, v) => {
          val variable = convertToVariable.invoke(null, k).asInstanceOf[sampleVariable.type]
          val value = convertToValue.invoke(null, v).asInstanceOf[sampleValue.type]
          env.put(variable, value)
        }
      }

      val theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment")
      theCaseInsensitiveEnvironmentField.setAccessible(true)
      val cienv = theCaseInsensitiveEnvironmentField.get(null).asInstanceOf[JavaMap[String, String]]
      cienv.putAll(newEnv)
    } catch {
      case e: NoSuchFieldException =>
        try {
          val classes = classOf[Collections].getDeclaredClasses()
          val env = System.getenv()
          for (cl <- classes) {
            if (cl.getName() == "java.util.Collections$UnmodifiableMap") {
              val field = cl.getDeclaredField("m")
              field.setAccessible(true)
              val obj = field.get(env)
              val map = obj.asInstanceOf[JavaMap[String, String]]
              map.putAll(newEnv)
            }
          }
        } catch {
          case e2: Exception => e2.printStackTrace()
        }

      case e1: Exception => e1.printStackTrace()
    }
  }

}
