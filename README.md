## Luigi Test

Just me testing out the `luigi` library for creating data pipelines.

Running the following command will fetch French election data, clean it, and produce a report in markdown.

`luigi --module tasks PresentData --local-scheduler --candidate HOLLANDE`

To start over:

`./clean.sh`