# Building project

## Targeting Java 11
Building project with Java 11 works in general, though `test-utils` module has a transient dependency (through `org.apache.curator-test:2.13.0`) on an older version of `javassist:3.18.1-GA` which results in following warning:
```bash
...
> Task :mesos-client:test
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by javassist.ClassPool (file:/Users/adukhovniy/.gradle/caches/modules-2/files-2.1/org.javassist/javassist/3.18.1-GA/d9a09f7732226af26bf99f19e2cffe0ae219db5b/javassist-3.18.1-GA.jar) to method java.lang.ClassLoader.defineClass(java.lang.String,byte[],int,int)
WARNING: Please consider reporting this to the maintainers of javassist.ClassPool
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
``` 

This is effectively due to the USI currently targeting Zookeeper 3.4.13 which is the version used by DC/OS 1.13. This issue should be resolved once DC/OS bumps its Zookeeper version.