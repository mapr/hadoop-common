package org.apache.hadoop.classification;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.SOURCE)
@Target(
    {
        ElementType.CONSTRUCTOR,
        ElementType.FIELD,
        ElementType.LOCAL_VARIABLE,
        ElementType.METHOD,
        ElementType.TYPE,
        ElementType.ANNOTATION_TYPE
    })
@MapRModified (summary = "Annotation to keep track of changes/additions made by MapR. " +
    "The retention policy is set to SOURCE so that it doesn't effect compilation or running." +
    "summary is set to empty string by default")
public @interface MapRModified {
  String summary() default "";
}
