# D ONE Flink Exercises

This is the repository of the Flink exercises!

Most of them are structured as JUnit tests, meaning there is a template of a test that you will need to modify, such
that the test will pass.

## Overview

```
 ├── AUTHORS.md
 ├── LICENSE
 ├── pom.xml
 ├── README.md                                   # This file!
 ├── src
 │  ├── main
 │  │  ├── java
 │  │  │  └── org
 │  │  │     └── done
 │  │  │        └── flink
 │  │  │           └── exercises
 │  │  │              ├── data
 │  │  │              │  ├── airq
 │  │  │              │  │  ├── AirQSensorData.java                 # Simple POJO that holds the measurements, the raw data.
 │  │  │              │  │  ├── AirQSensorDataSampleBuilder.java
 │  │  │              │  │  ├── generators         # Data generators, replays of recordings, or random.
 │  │  │              │  │  └── utils
 │  │  │              │  └── events                # Simple POJO that holds what we want to detect.
 │  │  │              └── DataStreamJob.java       # Basically empty, ignore.
 │  │  └── resources
 │  │     └── recordings                           # The raw recordings that we use in the exercises.
 │  └── test
 │     └── java
 │        └── org
 │           └── done
 │              └── flink
 │                 └── exercises
 │                    ├── exercises                 # The exercises! You will only need to touch the files in here.
 │                    ├── solutions                 # The solutions!
 │                    ├── support                   # Tests to check that everything is working. Has nothing to do with the test.
 │                    └── util                      # Utils for the exercises
 └── target
```


## How to run

1. Download this repository
2. Download [Intellij](https://www.jetbrains.com/idea/download/)
   In theory, other IDEs or editors will do the trick as well, but the tests do not currently run with just using `mvn test -Dtest="org.done.flink.exercises.exercises.**"`...
3. Open Intellij and then open this repository within.
4. Navigate to the exercise you want to do. 
5. Solve it! (There is no try.)
6. Test it! (Multiple options:)
   - click on the green arrow on the left of the code next to part you want to run, or 
   - click on the double arrow to run them all, or
   - in the file-browser on your left right-click on the folder, then select the green arrow where it says "Run 'Tests in ...'"
7. Marvel at your genius! (Or despair.)