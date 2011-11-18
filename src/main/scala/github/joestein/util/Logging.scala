package github.joestein.util

import org.apache.log4j.Logger;

/**
 * LogHelper is a trait you can mix in to provide easy log4j logging 
 * for your scala classes. 
 **/
trait LogHelper {
    val loggerName = this.getClass.getName
    lazy val logger = Logger.getLogger(loggerName)

	def debug(msg: => String) {
		if (logger.isDebugEnabled())
			logger.debug(msg)
	}
	def info(msg: => String) {
		if (logger.isInfoEnabled())
			logger.info(msg)
	}
	def error(msg: => String) {
		logger.error(msg)
	}		
	def fatal(msg: => String) {
		logger.fatal(msg)
	}	
	def warn(msg: => String) {
		logger.warn(msg)
	}	
}