javac src/ADBFinalProject/*.java
for filename in InputCases/*.txt 
do
    echo "Running Test Case : "$filename
    java -cp src ADBFinalProject.MainApp $filename
    echo "\n\n\n\n------------\n\n\n"
done
rm src/ADBFinalProject/*.class
