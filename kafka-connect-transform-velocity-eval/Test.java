import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;


public class Test {
	private static ScriptEngine engine;
	Object preparedExpression = null;
	
	static {
	    ScriptEngineManager factory = new ScriptEngineManager();
	    engine = factory.getEngineByName("JavaScript");
		try {
			FileReader jsonata = new FileReader("/Users/adeshmukh/kafka/jsonata/jsonata-es5.js");
		    engine.eval(jsonata);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Jsonata(String expression) throws NoSuchMethodException, Exception {
        Invocable inv = (Invocable) engine;
        try {
			preparedExpression = inv.invokeFunction("jsonata", expression);
		} catch (ScriptException e) {
			throw new Exception(e);
		}
	}
	
	public String evaluate(String jsonData) throws NoSuchMethodException, Exception {
		String output = null;
		if(preparedExpression != null) {
			try {
		        Invocable inv = (Invocable) engine;
		        engine.put("input", jsonData);
		        Object inputjson = engine.eval("JSON.parse(input);");
                Object resultjson = inv.invokeMethod(preparedExpression, "evaluate", inputjson);
                System.out.println(resultjson);
		        engine.put("resultjson", resultjson);
		        Object result = engine.eval("JSON.stringify(resultjson);");	
		        output = result.toString();
			} catch(ScriptException e) {
				throw new Exception(e);
			}
		}
        return output;
	}

	public static void main(String[] args) throws ScriptException, NoSuchMethodException, IOException {
        String expression = "$sum(Account.Order.Product.(Price * Quantity))";  // JSONata expression
        //String expression = "{'name': $.Account.`Account Name`}";  // JSONata expression
        expression = "$toMillis('21/6/2018', '[D#1]/[M#1]/[Y]') ~> $fromMillis('[D1o] [MNn] [Y]')";
	    byte[] sample = Files.readAllBytes(Paths.get("sample.json"));

        if(args.length > 0) {
            expression = args[0];
        }
	    String result = null;
		try {
		    Jsonata expr = new Jsonata(expression);
			result = expr.evaluate(new String(sample));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
        System.out.println(result);
	}
}