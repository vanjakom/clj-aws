package com.mungolab.aws.lambda;

import java.io.InputStream;
import java.io.OutputStream;

import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.lambda.runtime.Context;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

// lein uberjar
// export fn and ns or send as part of input json
// java -cp target/<APP>-standalone.jar com.mungolab.aws.lambda.TestLambda

public class TestLambda {
  public static void main(String[] args) {
    GenericHandler handler = new GenericHandler();

    IFn requireFn = Clojure.var("clojure.core", "require");
    requireFn.invoke(Clojure.read("clj-common.io"));

    IFn handlerFn = Clojure.var("clj-common.io", "string->input-stream");
    InputStream argAsStream = (InputStream)handlerFn.invoke(args[0]);

    System.out.println("Input:");
    System.out.println(args[0]);

    handler.handleRequest(argAsStream, System.out, null);
    System.out.println("");
  }
}
