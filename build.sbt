
lazy val AkkaStreamsKafka_master = (project in file("."))
  .aggregate(
    common,
    exercise_000_initial_state,
    exercise_001_actor_ref_with_ack,
    exercise_002_actor_ref_with_ack_scale,
    exercise_003_map_async
 ).settings(CommonSettings.commonSettings: _*)

lazy val common = project.settings(CommonSettings.commonSettings: _*)

lazy val exercise_000_initial_state = project
  .settings(CommonSettings.commonSettings: _*)
  .dependsOn(common % "test->test;compile->compile")

lazy val exercise_001_actor_ref_with_ack = project
  .settings(CommonSettings.commonSettings: _*)
  .dependsOn(common % "test->test;compile->compile")

lazy val exercise_002_actor_ref_with_ack_scale = project
  .settings(CommonSettings.commonSettings: _*)
  .dependsOn(common % "test->test;compile->compile")

lazy val exercise_003_map_async = project
  .settings(CommonSettings.commonSettings: _*)
  .dependsOn(common % "test->test;compile->compile")
       