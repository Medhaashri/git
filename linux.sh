Last login: Wed Mar 30 11:13:39 on ttys005
(base) medhaashriv@AGLS-MLT-562 ~ % nano video
(base) medhaashriv@AGLS-MLT-562 ~ % chmod +x video
(base) medhaashriv@AGLS-MLT-562 ~ % ./video
./video: line 1: AVI,: command not found
(base) medhaashriv@AGLS-MLT-562 ~ % 
(base) medhaashriv@AGLS-MLT-562 ~ % nano video
(base) medhaashriv@AGLS-MLT-562 ~ % 
(base) medhaashriv@AGLS-MLT-562 ~ % nano video
(base) medhaashriv@AGLS-MLT-562 ~ % nano video.txt
(base) medhaashriv@AGLS-MLT-562 ~ % nano audio.csv
(base) medhaashriv@AGLS-MLT-562 ~ % nano vdocuments.txt
(base) medhaashriv@AGLS-MLT-562 ~ % nano image.dat
(base) medhaashriv@AGLS-MLT-562 ~ % nano other.csv
(base) medhaashriv@AGLS-MLT-562 ~ % ls *.txt 
datafile.txt	ms.txt		new.txt		vdocuments.txt	video.txt
(base) medhaashriv@AGLS-MLT-562 ~ % ls _d v*
ls: _d: No such file or directory
vdocuments.txt	video		video.txt
(base) medhaashriv@AGLS-MLT-562 ~ % grep _l MP4 *.txt
grep: MP4: No such file or directory
(base) medhaashriv@AGLS-MLT-562 ~ % ls |grep -v 'csv'
1st que.ipynb
2nd question.ipynb
Applications
Desktop
Documents
Downloads
Library
Movies
Music
OneDrive - Agilisium Consulting India Private Limited
PM_OF_INDIA.ipynb
Pictures
Public
PycharmProjects
Untitled Folder
Untitled.ipynb
Untitled1.ipynb
Untitled2.ipynb
Untitled3.ipynb
Untitled4.ipynb
Untitled5.ipynb
Untitled6.ipynb
Untitled7.ipynb
book1 (1) (1).ipynb
datafile.txt
file1.xlsx
first note.ipynb
image.dat
ms.txt
new.txt
opt
pm_india.py
python to sql.ipynb
vdocuments.txt
video
video.txt
web scrapping final output.ipynb
(base) medhaashriv@AGLS-MLT-562 ~ % nano video.txt
(base) medhaashriv@AGLS-MLT-562 ~ % grep _l MP4 *.txt
grep: MP4: No such file or directory
(base) medhaashriv@AGLS-MLT-562 ~ % ls |grep _l MP4 *.txt
grep: MP4: No such file or directory
(base) medhaashriv@AGLS-MLT-562 ~ % grep -1 MP4 *.*
audio.csv:MP3, WAV, OGG, MP4
--
other.csv:CSV, JSON, XML, HTML, ZIP, MP4
other.csv-
--
video.txt- ZIP,
video.txt: MP4
video.txt-
(base) medhaashriv@AGLS-MLT-562 ~ % cd desktop
(base) medhaashriv@AGLS-MLT-562 desktop % cd linux
(base) medhaashriv@AGLS-MLT-562 linux % cat> video.txt
AVI, MOV, MP4, OGG, WMV, WEBM
(base) medhaashriv@AGLS-MLT-562 linux % cat> audio.csv
 MP3, WAV, OGG, MP4
(base) medhaashriv@AGLS-MLT-562 linux % cat>vdocuments.txt
DOC, DOCX, XLS, XLSX, PPT, PPTX, PDF, ODT, ODS, ODP, RTF


(base) medhaashriv@AGLS-MLT-562 linux % image.dat
zsh: command not found: image.dat
(base) medhaashriv@AGLS-MLT-562 linux % cat>image.dat
Images: JPG, PNG, GIF, TIFF, ICO, SVG, WEBP


(base) medhaashriv@AGLS-MLT-562 linux % cat>others.csv
Other: CSV, JSON, XML, HTML, ZIP, MP4


(base) medhaashriv@AGLS-MLT-562 linux % ls
audio.csv	image.dat	others.csv	vdocuments.txt	video.txt
(base) medhaashriv@AGLS-MLT-562 linux % ls *.txt 
vdocuments.txt	video.txt
(base) medhaashriv@AGLS-MLT-562 linux % ls v*
vdocuments.txt	video.txt
(base) medhaashriv@AGLS-MLT-562 linux % grep Ril "MP4"
grep: MP4: No such file or directory
(base) medhaashriv@AGLS-MLT-562 linux % grep -1 MP4 *.*
audio.csv: MP3, WAV, OGG, MP4
--
others.csv:Other: CSV, JSON, XML, HTML, ZIP, MP4
others.csv-
--
video.txt:AVI, MOV, MP4, OGG, WMV, WEBM
(base) medhaashriv@AGLS-MLT-562 linux % ls |grep -v
usage: grep [-abcdDEFGHhIiJLlMmnOopqRSsUVvwXxZz] [-A num] [-B num] [-C[num]]
	[-e pattern] [-f file] [--binary-files=value] [--color=when]
	[--context[=num]] [--directories=action] [--label] [--line-buffered]
	[--null] [pattern] [file ...]
(base) medhaashriv@AGLS-MLT-562 linux % ls |grep _v "csv"
grep: csv: No such file or directory
(base) medhaashriv@AGLS-MLT-562 linux % ls
audio.csv	image.dat	others.csv	vdocuments.txt	video.txt
(base) medhaashriv@AGLS-MLT-562 linux % ls |grep -v 'csv'
image.dat
vdocuments.txt
video.txt
(base) medhaashriv@AGLS-MLT-562 linux %     
