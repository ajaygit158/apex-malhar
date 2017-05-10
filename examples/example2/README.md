The DAG for this example is
fileReader -->  lineCounter --> ConsoleOutputOperator

It will read a directory containing files. Each line is emitted as a record. For this application, a file is representing a batch. The line counter will count the number of lines in each file in a batch and emit this count. The console output operator stdout's the received count.

