package org.jsonata;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class Jsonata {
  private static ScriptEngine engine;
  Object preparedExpression;

  static {
    ScriptEngineManager factory = new ScriptEngineManager();
    engine = factory.getEngineByName("JavaScript");
    try {
      engine.eval(new BufferedReader(
        new InputStreamReader(Jsonata.class.getClassLoader().getResourceAsStream("jsonata-es5.js"))));
    } catch(ScriptException e) {
      throw new RuntimeException(e);
    }
  }

  public Jsonata(String expression) throws NoSuchMethodException, ScriptException {
    preparedExpression = ((Invocable) engine).invokeFunction("jsonata", expression);
  }

  public String evaluate(String jsonData) throws NoSuchMethodException, ScriptException {
    if (preparedExpression != null) {
      engine.put("input", jsonData);
      Object inputjson = engine.eval("JSON.parse(input);");
      Object resultjson = ((Invocable) engine).invokeMethod(preparedExpression, "evaluate", inputjson);
      engine.put("resultjson", resultjson);
      return engine.eval("JSON.stringify(resultjson);").toString();
    }
    return null;
  }
}