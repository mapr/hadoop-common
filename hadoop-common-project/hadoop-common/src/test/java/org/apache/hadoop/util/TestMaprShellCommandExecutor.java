package org.apache.hadoop.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestMaprShellCommandExecutor {
  private MaprShellCommandExecutor executor;

  @Before
  public void setUp() {
    this.executor = new MaprShellCommandExecutor();
  }

  @Test
  public void testCheckCommandCreation() {
    final String[] expected = new String[] {"maprcli", "node", "list", "-json"};

    String[] args = executor.createArgs(new String[] {"node", "list"}, new HashMap<String, String>());

    assertArrayEquals(expected, args);
  }

  @Test
  public void testCheckParams() {
    final Set<String> expected = new HashSet<>(Arrays.asList("maprcli", "node", "list", "-param1", "value1", "-param2", "value2", "-json"));

    String[] commands = new String[] {"node", "list"};
    Map<String, String> keyParamsWithoutMinus = new HashMap<>();
    keyParamsWithoutMinus.put("param1", "value1");
    keyParamsWithoutMinus.put("param2", "value2");
    String[] args = executor.createArgs(commands, keyParamsWithoutMinus);

    assertEquals(expected.size(), args.length);
    assertTrue(expected.containsAll(Arrays.asList(args)));

    Map<String, String> keyParamsWithMinus = new HashMap<>();
    keyParamsWithMinus.put("-param1", "value1");
    keyParamsWithMinus.put("-param2", "value2");
    args = executor.createArgs(commands, keyParamsWithMinus);

    assertEquals(expected.size(), args.length);
    assertTrue(expected.containsAll(Arrays.asList(args)));
  }

  @Test
  public void testCheckEmptyParameterMap() {
    final String[] expected = new String[] {"maprcli", "node", "list", "-json"};

    String[] args = executor.createArgs(new String[] {"node", "list"}, null);

    assertArrayEquals(expected, args);

    args = executor.createArgs(new String[] {"node", "list"}, new HashMap<String, String>());

    assertArrayEquals(expected, args);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckEmptyCommand() {
    executor.createArgs(null, null);
  }

  @Test
  public void testCheckOutput() throws IOException {
    Shell.ShellCommandExecutor mock = mock(Shell.ShellCommandExecutor.class);

    when(mock.getOutput()).thenReturn(getFileFromClassPath("command.json"));
    executor.setCommandExecutor(mock);

    JsonArray responseArray = executor.execute(new String[] {"test"}, new HashMap<String, String>());

    for (JsonElement e : responseArray) {
      JsonPrimitive hostname = e.getAsJsonObject().getAsJsonPrimitive("hostname");
      assertTrue(hostname.getAsString().contains("node"));
    }
  }

  @Test
  public void testCheckEmptyJsonOutput() throws IOException {
    Shell.ShellCommandExecutor mock = mock(Shell.ShellCommandExecutor.class);

    when(mock.getOutput()).thenReturn("{}");
    executor.setCommandExecutor(mock);

    executor.execute(new String[] {"test"}, new HashMap<String, String>());
  }

  @Test(expected = IOException.class)
  public void testCheckEmptyOutput() throws IOException {
    Shell.ShellCommandExecutor mock = mock(Shell.ShellCommandExecutor.class);

    when(mock.getOutput()).thenReturn("");
    executor.setCommandExecutor(mock);

    executor.execute(new String[] {"test"}, new HashMap<String, String>());
  }

  private String getFileFromClassPath(String fileName) throws IOException {
    InputStream fileStream = getClass().getClassLoader().getResourceAsStream(fileName);
    assertNotNull("File " + fileName + " not exists", fileStream);
    return IOUtils.toString(fileStream);
  }
}