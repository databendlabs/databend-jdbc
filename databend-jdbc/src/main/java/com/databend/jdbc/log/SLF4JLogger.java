package com.databend.jdbc.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SLF4JLogger implements DatabendLogger {

	private final Logger logger;

	public SLF4JLogger(String name) {
		logger = LoggerFactory.getLogger(name);
	}

	@Override
	public void trace(String message) {
		logger.trace(message);
	}

	@Override
	public void trace(String message, Object... arguments) {
		logger.trace(message, arguments);
	}

	@Override
	public void trace(String message, Throwable t) {
		logger.trace(message, t);
	}

	@Override
	public void debug(String message) {
		logger.debug(message);
	}

	@Override
	public void debug(String message, Object... arguments) {
		logger.debug(message, arguments);

	}

	@Override
	public void debug(String message, Throwable t) {
		logger.debug(message, t);
	}

	@Override
	public void info(String message) {
		logger.info(message);
	}

	@Override
	public void info(String message, Object... arguments) {
		logger.info(message, arguments);
	}

	@Override
	public void info(String message, Throwable t) {
		logger.info(message, t);
	}

	@Override
	public void warn(String message) {
		logger.warn(message);
	}

	@Override
	public void warn(String message, Object... arguments) {
		logger.warn(message, arguments);
	}

	@Override
	public void warn(String message, Throwable t) {
		logger.warn(message, t);
	}

	@Override
	public void error(String message) {
		logger.error(message);
	}

	@Override
	public void error(String message, Object... arguments) {
		logger.error(message, arguments);
	}

	@Override
	public void error(String message, Throwable t) {
		logger.error(message, t);
	}
}
