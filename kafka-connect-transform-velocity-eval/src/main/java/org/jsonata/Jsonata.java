package org.jsonata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URL;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class Jsonata {
  private ScriptEngine engine;
  Object preparedExpression;

  public Jsonata(String expression) throws NoSuchMethodException, ScriptException, FileNotFoundException {
    ScriptEngineManager factory = new ScriptEngineManager();
    engine = factory.getEngineByName("JavaScript");
    engine.eval(new BufferedReader(
        new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("jsonata-es5.js"))));
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