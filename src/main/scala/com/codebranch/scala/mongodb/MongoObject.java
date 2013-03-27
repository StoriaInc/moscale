package com.codebranch.scala.mongodb;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;



@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface MongoObject
{
	boolean javaStyle() default true;
	boolean scalaStyle() default true;
}
