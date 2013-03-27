package com.codebranch.scala.mongodb;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;



/** User: alexey Date: 10/5/12 Time: 1:46 PM */

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface CollectionEntity
{
	String databaseName();
	String collectionName();
}
