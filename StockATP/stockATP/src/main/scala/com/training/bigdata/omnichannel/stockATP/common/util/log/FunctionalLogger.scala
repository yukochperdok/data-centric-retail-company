package com.training.bigdata.omnichannel.stockATP.common.util.log

import org.slf4j.LoggerFactory

private object LogType extends Enumeration {
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
  val SubprocessBeginning = Value("I")

  /**
    * F
    * Indicar la finalización de alguno de los "subprocesamientos" internos de nuestra aplicación, ya sea la lectura de una tabla,
    * escritura, transformación, etc.
    * Gracias a este log y al de finalización, podríamos obtener más precisamente, qué subproceso de nuestra aplicación consume
    * más tiempo y optimizarlo (siempre teniendo en cuenta la evaluación lazy de Spark)
    */
  val SubprocessEnd = Value("F")

  /**
    * FP
    * Indica la finalización completa del proceso principal.
    * Deberá ser un log situado al final de la ejecución de la aplicación (junto al log de tiempo)
    * Gracias a este log se podría detectar la ejecución correcta (sin fallos técnicos) del proceso/aplicación.
    */
  val ProcessEnd = Value("FP")

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

  private[this] def getPrecededString(logType: LogType.Value): String = {
    s"$logType - "
  }

  private[this] def debug(message: => String, logType: LogType.Value) =
    if (buildLogger.isDebugEnabled) buildLogger.debug(s"${getPrecededString(logType)}$message")

  private[this] def info(message: => String, logType: LogType.Value) =
    if (buildLogger.isInfoEnabled) buildLogger.info(s"${getPrecededString(logType)}$message")

  private[this] def warn(message: => String, logType: LogType.Value) =
    if (buildLogger.isWarnEnabled) buildLogger.warn(s"${getPrecededString(logType)}$message")

  private[this] def error(message: => String, logType: LogType.Value) =
    if (buildLogger.isErrorEnabled) buildLogger.error(s"${getPrecededString(logType)}$message")

  def logDebug(message: => String): Unit = debug(message, LogType.Message)
  def logInfo(message: => String): Unit = info(message, LogType.Message)
  def logWarn(message: => String): Unit = warn(message, LogType.Warning)

  def logEndProcess(message: => String, duration: Long) = {
    info(s"$duration", LogType.TotalProcessTime)
    info(message, LogType.ProcessEnd)
  }
  def logBeginSubprocess(message: => String) = info(message, LogType.SubprocessBeginning)
  def logEndSubprocess(message: => String) = info(message, LogType.SubprocessEnd)
  def logAlert(message: => String) = warn(message, LogType.Alert)
  def logConfig(message: => String) = info(message, LogType.Configuration)
  def logNonFunctionalMessage(message: => String) = buildLogger.error(message)

  /**
    * Log the block of code belongs to sub-process
    *
    * @param nameSubProcess name of sub-process to log
    * @param block code to execute
    * @return execute the block of code belongs to process and log it
    */
  def logSubProcess[T](nameSubProcess:String)(block: => T): T = {
    logBeginSubprocess(s"Call ${nameSubProcess}")
    val signal = block
    logEndSubprocess(s"Call ${nameSubProcess} finished correctly")
    signal
  }

  /**
    * Log the block of code belongs to functional-process
    *
    * @param nameProcess name of process to log
    * @param block code to execute
    * @return execute the block of code belongs to process and log it
    */
  def logProcess(nameProcess:String)(block: => Unit) = {
    val t0: Long = System.currentTimeMillis()
    block
    val t1: Long = System.currentTimeMillis()
    logEndProcess(s"${nameProcess} finished correctly", t1 - t0)
  }


}