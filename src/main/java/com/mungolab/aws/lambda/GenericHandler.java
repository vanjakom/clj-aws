package com.mungolab.aws.lambda;

import java.io.InputStream;
import java.io.OutputStream;

import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.lambda.runtime.Context;

import clojure.java.api.Clojure;
import clojure.lang.IFn;


public class GenericHandler implements RequestStreamHandler {
  public void handleRequest(InputStream inputStream,
                            OutputStream outputStream,
                            Context context) {
    IFn requireFn = Clojure.var("clojure.core", "require");
    requireFn.invoke(Clojure.read("clj-aws.lambda"));

    IFn handlerFn = Clojure.var("clj-aws.lambda", "generic-handler");
    handlerFn.invoke(inputStream, outputStream, context);
  }
}
