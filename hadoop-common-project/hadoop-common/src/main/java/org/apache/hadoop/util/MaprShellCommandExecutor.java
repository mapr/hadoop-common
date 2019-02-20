package org.apache.hadoop.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A base class for running a maprcli unix command.
 * By defaults add maprcli prefix and -json suffix to all commands
 */
public class MaprShellCommandExecutor {
  private static final Log LOG = LogFactory.getLog(MaprShellCommandExecutor.class);
  private static final String DATA_FIELD = "data";

  private final JsonParser parser = new JsonParser();
  private Shell.ShellCommandExecutor executor;

  /**
   * Call maprcli command on cluster.
   *
   * @param command commands passed by client
   * @param params  extra command params
   * @return json object which contains response data
   * @throws IOException
   */
  public JsonArray execute(String[] command, Map<String, String> params) throws IOException {
    if (executor == null) {
      executor = new Shell.ShellCommandExecutor(createArgs(command, params));
    }
    LOG.info("Trying to execute maprcli command: " + executor.toString());
    try {
      executor.execute();
      String output = executor.getOutput();
      if (output == null || output.isEmpty()) {
        LOG.error("Output is empty");
        throw new IOException("Empty output");
      }
      return parser.parse(executor.getOutput()).getAsJsonObject().getAsJsonArray(DATA_FIELD);
    } catch (IOException e) {
      int exitCode = executor.getExitCode();
      if (exitCode != 0) {
        LOG.error("Failed to execute command. Command output " + executor.getOutput());
      }
      throw e;
    } finally {
      executor = null;
    }
  }

  @VisibleForTesting
  protected void setCommandExecutor(Shell.ShellCommandExecutor executor) {
    this.executor = executor;
  }

  /**
   * @param command commands passed by client
   * @param params  command parameters
   * @return full maprcli command
   */
  @VisibleForTesting
  protected String[] createArgs(String[] command, Map<String, String> params) {
    if (command == null) {
      throw new IllegalArgumentException("Empty command");
    }
    if (params == null) {
      params = new HashMap<>();
    }

    // extra fields -> maprcli command and -json flag
    int size = command.length + (params.size() * 2) + 2;
    String[] args = new String[size];

    int counter = 0;
    args[counter++] = "maprcli";
    for (String c : command) {
      args[counter++] = c;
    }

    for (Map.Entry<String, String> e : params.entrySet()) {
      String paramName = validateAndGetParamName(e.getKey());
      args[counter++] = paramName;
      args[counter++] = e.getValue();
    }
    args[counter] = "-json";
    return args;
  }

  /**
   * Check value and return parameter with - (minus) prefix.
   *
   * @param value parameter value
   * @return parameter with - (minus) prefix e.g. -name
   */
  private String validateAndGetParamName(String value) {
    if (value.startsWith("-")) {
      return value;
    } else {
      return "-" + value;
    }
  }
}
