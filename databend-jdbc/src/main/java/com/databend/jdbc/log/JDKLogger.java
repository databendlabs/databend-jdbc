package com.firebolt.jdbc.log;

import java.util.logging.Level;

public class JDKLogger implements FireboltLogger {

	private final java.util.logging.Logger logger;

	public JDKLogger(String name) {
		this.logger = java.util.logging.Logger.getLogger(name);
	}

	@Override
	public void trace(String message) {
		logger.log(Level.FINEST, message);
	}

	@Override
	public void trace(String message, Object... arguments) {
		logger.log(Level.FINEST, addMissingArgumentsIndexes(message), arguments);
	}

	@Override
	public void trace(String message, Throwable t) {
		logger.log(Level.FINEST, message, t);
	}

	@Override
	public void debug(String message) {
		logger.log(Level.FINE, message);
	}

	@Override
	public void debug(String message, Object... arguments) {
		logger.log(Level.FINE, addMissingArgumentsIndexes(message), arguments);
	}

	@Override
	public void debug(String message, Throwable t) {
		logger.log(Level.FINE, message, t);
	}

	@Override
	public void info(String message) {
		logger.log(Level.INFO, message);
	}

	@Override
	public void info(String message, Object... arguments) {
		logger.log(Level.INFO, addMissingArgumentsIndexes(message), arguments);
	}

	@Override
	public void info(String message, Throwable t) {
		logger.log(Level.INFO, message, t);
	}

	@Override
	public void warn(String message) {
		logger.log(Level.WARNING, message);
	}

	@Override
	public void warn(String message, Object... arguments) {
		logger.log(Level.WARNING, addMissingArgumentsIndexes(message), arguments);
	}

	@Override
	public void warn(String message, Throwable t) {
		logger.log(Level.WARNING, message, t);

	}

	@Override
	public void error(String message) {
		logger.log(Level.SEVERE, message);
	}

	@Override
	public void error(String message, Object... arguments) {
		logger.log(Level.SEVERE, addMissingArgumentsIndexes(message), arguments);
	}

	@Override
	public void error(String message, Throwable t) {
		logger.log(Level.SEVERE, message, t);
	}

	/**
	 * SLF4J and java.util.logging use a different log format. With SLF4J it is not
	 * required to have argument indexes in the logs (eg: "log.info("hello {}",
	 * "world");), but it is required for java.util.logging (eg: "log.info("hello
	 * {1}", "world");) In this project we use the SLF4J way of logging, which is
	 * why we need to add the missing indexes.
	 */
	private String addMissingArgumentsIndexes(String message) {
		StringBuilder result = new StringBuilder();
		int argumentIndex = 0;
		int i = 0;
		while (i < message.length()) {
			if (message.charAt(i) == '{' && i < message.length() - 1 && message.charAt(i + 1) == '}') {
				result.append(String.format("{%d}", argumentIndex++));
				i++;
			} else {
				result.append(message.charAt(i));
			}
			i++;
		}
		return result.toString();
	}
}
