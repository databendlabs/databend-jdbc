package com.databend.jdbc.log;

public interface DatabendLogger {

	void trace(String message);

	void trace(String message, Object... arguments);

	void trace(String message, Throwable t);

	void debug(String message);

	void debug(String message, Object... arguments);

	void debug(String message, Throwable t);

	void info(String message);

	void info(String message, Object... arguments);

	void info(String message, Throwable t);

	void warn(String message);

	void warn(String message, Object... arguments);

	void warn(String message, Throwable t);

	void error(String message);

	void error(String message, Object... arguments);

	void error(String message, Throwable t);

}
