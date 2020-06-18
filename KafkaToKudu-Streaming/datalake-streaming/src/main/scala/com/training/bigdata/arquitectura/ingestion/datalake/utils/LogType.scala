package com.training.bigdata.ingestion.utils

import java.util.Optional

import org.slf4j.{Logger, LoggerFactory}

/**
  * Set monitorization type
  **/
object MonitorType extends Enumeration {
  /**
    * Fix logs as monitorization log
    **/
  val Functional = Value("F")
  /**
    * Fix logs as standard logs
    */
  val Standard = Value("S")

}


object LogType extends Enumeration {
  /**
    * A
    * Este caso se usará exclusivamente para indicar que la aplicación debería generar una ALERTA en ServiceManager.
    * A futuro estos mensajes deberían tener un estándar o ser prefijados para ser fácilmente reconocibles.
    */
  val Alert = Value("A")

  /**
    * W
    * Generación de mensajes de WARNING's; posteriormente la detección de repetidos y continuados mensajes de warning
    * (que comiencen con "W") sobre las diferentes ejecuciones de una misma malla/proceso, podrán ser casos de usos para la
    * generación de ALERTAS.
    */
  val Warning = Value("W")

  /**
    * I
    * Indicar el comienzo de alguno de los "subprocesamientos" internos de nuestra aplicación, ya sea la lectura de una tabla,
    * escritura, transformación, etc.
    * Gracias a este log y al de finalización, podríamos obtener más precisamente, qué subproceso de nuestra aplicación consume
    * más tiempo y optimizarlo (siempre teniendo en cuenta la evaluación lazy de Spark)
    */
  val InicioSubproceso = Value("I")

  /**
    * F
    * Indicar la finalización de alguno de los "subprocesamientos" internos de nuestra aplicación, ya sea la lectura de una tabla,
    * escritura, transformación, etc.
    * Gracias a este log y al de finalización, podríamos obtener más precisamente, qué subproceso de nuestra aplicación consume
    * más tiempo y optimizarlo (siempre teniendo en cuenta la evaluación lazy de Spark)
    */
  val FinSubproceso = Value("F")

  /**
    * T
    * Este log indicará, única y exclusivamente el tiempo en milisegundos que ha tardado la ejecución del proceso.
    * Deberá ser un log situado al final de la ejecución de la aplicación.
    * Su objetivo es el de poder generar gráficas en cuadros de mando de monitorización, así como observar el rendimiento de los procesos.
    */
  val TotalProcessTime = Value("T")

  /**
    * C
    * Indica el establecimiento de las variables de configuración y parámetros a usar en la ejecución de nuestra aplicación.
    * Normalmente irán situados al principio del log, en la parte de la lectura de configuración de nuestra aplicación.
    */
  val Configuration = Value("C")

  /**
    * M
    * Indica mensaje común para monitorización técnica y/o funcional de la ejecución de nuestra aplicación.
    */
  val Message = Value("M")
}


trait FunctionalLogger extends Serializable {

  @transient
  lazy val buildLogger = LoggerFactory.getLogger(getClass)

  private def getPrecededString(logType: LogType.Value): String = {
    s"$logType - "
  }

  def debug(message: => String) = if (buildLogger.isDebugEnabled) buildLogger.debug(messageBuilder(message))

  def info(message: => String, logType: LogType.Value = LogType.Message, monType: MonitorType.Value = MonitorType.Standard) = if (buildLogger.isInfoEnabled) buildLogger.info(messageBuilder(message, logType, monType))

  def warn(message: => String, logType: LogType.Value = LogType.Message, monType: MonitorType.Value = MonitorType.Standard) = if (buildLogger.isWarnEnabled) buildLogger.warn(messageBuilder(message, logType, monType))

  def error(message: => String, throwable: Throwable = null): Unit = if (buildLogger.isErrorEnabled) (if (throwable != null) buildLogger.error(messageBuilder(message), throwable) else buildLogger.error(messageBuilder(message)))



  private def messageBuilder(message: => String, logType: LogType.Value = LogType.Message, monType: MonitorType.Value = MonitorType.Standard, except: Optional[Throwable] = Optional.empty()): String = {
    //Format message structure
    val outputMessage = monType match {
      case MonitorType.Standard => s"[${Thread.currentThread.getStackTrace.toList(4).getLineNumber.toString}] $message"
      case MonitorType.Functional => s"${getPrecededString(logType)}[${Thread.currentThread.getStackTrace.toList(4).getLineNumber.toString}] $message"
    }
    outputMessage
  }

  private def publishLog(publishLogger: Logger, methodName: String, message: => String, logType: LogType.Value, monType: MonitorType.Value, except: Optional[Throwable] = Optional.empty()): Unit = {
    //Format message structure
    val outputMessage = monType match {
      case MonitorType.Standard => s"[${Thread.currentThread.getStackTrace.toList(4).getLineNumber.toString}] $message"
      case MonitorType.Functional => s"${getPrecededString(logType)}[${Thread.currentThread.getStackTrace.toList(4).getLineNumber.toString}] $message"
    }

    val currentMethod = if (except.isPresent) publishLogger.getClass.getDeclaredMethod(methodName, classOf[String], classOf[Throwable]) else publishLogger.getClass.getDeclaredMethod(methodName, classOf[String])
    if (except.isPresent) {
      currentMethod.invoke(publishLogger, outputMessage, except.get())
    } else {
      currentMethod.invoke(publishLogger, outputMessage)
    }
  }

  /**
    * Measure the time of a block
    *
    * @param block code to execute
    * @return milliseconds
    */
  def time(block: => Unit): Long = {
    val t0 = System.currentTimeMillis()
    block // call-by-name
    val t1 = System.currentTimeMillis()
    t1 - t0
  }


}








