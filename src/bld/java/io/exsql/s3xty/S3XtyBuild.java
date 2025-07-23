package io.exsql.s3xty;

import rife.bld.Project;
import rife.bld.dependencies.Version;

import java.util.List;

import static rife.bld.dependencies.Repository.*;
import static rife.bld.dependencies.Scope.*;

public class S3XtyBuild extends Project {

    public S3XtyBuild() {
        pkg = "io.exsql.s3xty";
        name = "s3xty";
        mainClass = "io.exsql.s3xty.S3Xty";
        version = version(0,1,0);

        downloadSources = true;
        repositories = List.of(MAVEN_CENTRAL, RIFE2_RELEASES);
        scope(compile)
                .include(dependency("it.unimi.dsi", "fastutil", version(8, 5, 16)))
                .include(dependency("com.google.guava", "guava", Version.parse("33.4.8-jre")))
                .include(dependency("com.google.re2j", "re2j", version(1, 8)));

        scope(provided)
                .include(dependency("org.apache.spark", "spark-sql_2.13", version(3,5,6)));

        scope(test)
            .include(dependency("org.junit.jupiter", "junit-jupiter", version(5,11,4)))
            .include(dependency("org.junit.platform", "junit-platform-console-standalone", version(1,11,4)));

        compileOperation()
                .compileOptions()
                .addModules("jdk.incubator.vector");
    }

    public static void main(final String[] args) {
        new S3XtyBuild().start(args);
    }

}