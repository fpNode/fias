

dir -r -i *.xsd -fo | % {
   & "C:\Program Files (x86)\Microsoft SDKs\Windows\v8.1A\bin\NETFX 4.5.1 Tools\xsd.exe" $_.fullname /c /n:fpNode.FIAS.Models /o:../../Models 
}