package org.jsonata;

import java.io.FileNotFoundException;
import java.io.FileReader;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class Jsonata {
  private ScriptEngine engine;
  Object preparedExpression;

  public Jsonata(String expression) throws NoSuchMethodException, ScriptException, FileNotFoundException {
    FileReader jsonata = new FileReader("jsonata-es5.js");
    ScriptEngineManager factory = new ScriptEngineManager();
    engine = factory.getEngineByName("JavaScript");
    engine.eval(jsonata);
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