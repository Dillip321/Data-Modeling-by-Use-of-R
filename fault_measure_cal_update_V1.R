library("openNLP")
library("openNLPmodels.en")
library("tm")
library("gdata")
## Need to change paths for taxonomy, claimant_report & Police  Reports
##
fun1=function(sp){
  i_sub=grep("/N",sp)
  if(length(i_sub)>0)
  {i_sub1=min(i_sub)
   sub=sp[i_sub1]
   sp1=sp[i_sub1:length(sp)][-1]
   if(length(sp1)>0)
   {i_pre=grep("/V",sp1)
    if(length(i_pre)>0)
    {i_pre1=max(i_pre)
     pre=sp1[i_pre1]
     sp2=sp1[i_pre1:length(sp1)][-1]
     if(length(sp2)>0)
     {i_obj_n=grep("/N",sp2)
      i_obj_a=grep("/J",sp2)
      if(length(i_obj_n)>0)
      { obj=sp2[min(i_obj_n)]
      }else{if(length(i_obj_a)>0)
      {obj=sp2[min(i_obj_a)]
      }else{obj=""}
      }
     }else{obj=""}
    }else{pre="";obj=""}
   }else{pre="";obj=""}
  }else{sub="";pre="";obj=""}
  data.frame(subject=sub,predicate=pre,object=obj)
}

fun2=function(x)
{var1=as.character(x)
 var2=paste(do.call(rbind,strsplit((strsplit(var1
                                             ," ")[[1]]),"/"))[,1],collapse=" ")
 var2=gsub('\\.','',var2)
 var2=gsub(';','',var2)
 var2=gsub(':','',var2)
}

##
fun3=function(x){
  stm_tax=stemDocument(x)
  pre_obj=as.character(c(output[,3],output[,4]))
  char=sapply(pre_obj,nchar)
  pre_obj=pre_obj[char>0]
  pre_obj=tolower(pre_obj)
  stm_PO=stemDocument(pre_obj)
  freq=sapply(stm_tax,function(x){count=sum(stm_PO==x)})
  ind=which(freq>0)
  if(length(ind)>0)
  {word=stm_tax[ind];hit=freq[ind]
  }else{word=" ";hit=0}
  xo=data.frame(phrase=word,hits=hit);row.names(xo)=NULL
  xo
}

fun4=function(x){
  out1=output[,2:4]
  out1=tolower(out1)
  out1=trim(out1)
  ind1=which(as.character(out1[,1])==tolower(name[1]))
  ind2=which(as.character(out1[,1])==tolower(name[2]))
  if(length(ind1)>0)
  {unit1_PO=c(out1[ind1,2],out1[ind1,3])
   st_unit1=stemDocument(unit1_PO)
   count1=sapply(x,function(x){sum(st_unit1==x)})
   freq1=sum(count1)
  }else{(freq1=0)}
  if(length(ind2)>0)
  {unit2_PO=c(out1[ind2,2],out1[ind2,3])
   st_unit2=stemDocument(unit2_PO)
   count2=sapply(x,function(x){count=sum(st_unit2==x)})
   freq2=sum(count2)
  }else{(freq2=0)}
  x=data.frame(unit1=freq1,unit2=freq2)
  colnames(x)=name
  x
}


fun5=function(report){
  report=gsub("[\x01-\x1f\x7f-\xff]", "",report)
  report=gsub("/", "-",report)
  sent=unlist(lapply(report,sentDetect))
  tp=tagPOS(sent)
  ssp=strsplit(tp," ")
  for(i in 2:length(ssp))
  {
    x=ssp[[i]]
    ind1=grep("/PRP",x)
    if(length(ind1)>0)
    {y=ssp[[(i-1)]]
     ind2=grep("/NNP",y)
     if(length(ind2)>0)
     {ssp[[i]][min(ind1)]=y[min(ind2)]}
    }
  }
  out=lapply(ssp,fun1)
  out=do.call(rbind,out)
  out=apply(out,c(1,2),fun2)
  output<<-cbind(sentence=sent,out)
  output<<-apply(output,c(1,2),function(x){gsub(',','',x)})
  
  #########
  taxonomy=as.character(read.table("/home/hduser/subro/taxonomy.txt")[,1])
  #########  
  
  No_of_hits_on_taxonomy_phrases<<-fun3(taxonomy)
  total_hits=sum(No_of_hits_on_taxonomy_phrases[,2])
  New=data.frame(phrase="Total",hits=total_hits)
  No_of_hits_on_taxonomy_phrases<<-rbind(No_of_hits_on_taxonomy_phrases,New)
  hit_attribution_fault_Measure<<-fun4(as.character(No_of_hits_on_taxonomy_phrases[,1]))
  n1=hit_attribution_fault_Measure[1,1]
  n2=hit_attribution_fault_Measure[1,2]
  N=n1+n2
  hit_attribution_fault_Measure<<-rbind(hit_attribution_fault_Measure,c(n1/total_hits,n2/total_hits)*100)
  hit_attribution_fault_Measure<<-cbind(Title=c("Hit Count","Fault Measure (%)"),hit_attribution_fault_Measure)
  hit_attribution_fault_Measure<<-cbind(hit_attribution_fault_Measure,Non_Attributable=c((total_hits-N),((total_hits-N)*100/total_hits)))
}
###

################################################################################################################
############################ Parameters for R Code #############################################################

#Reading police report
report=scan("/home/hduser/subro/claimant_report.txt", what = "character", sep = "?")
# Parameters to Search in Subject phrase 
name=c("Party1","Party2")

if(file.exists(paste("/home/hduser/subro/Reports/",report,".txt",sep="")))
{
  #########
  pr=scan(paste("/home/hduser/subro/Reports/",report,".txt",sep=""),what="character",sep="?")
  #########
  fun5(pr)
  write.table(output,"Text_Analysis1.txt",row.names=F,col.names=F,quote=F,sep=",")
  write.table(No_of_hits_on_taxonomy_phrases,"Text_Analysis2.txt",row.names=F,col.names=F,quote=F,sep=",")
  write.table(hit_attribution_fault_Measure,"Text_Analysis3.txt",row.names=F,col.names=F,quote=F,sep=",") 
}else{
  write.table(data.frame(Note="Police Report is not exist"),"Text_Analysis1.txt",row.names=F,col.names=F,quote=F,sep=",")
  write.table(NULL,"Text_Analysis2.txt",row.names=F,quote=F,col.names=F,sep=",")
  write.table(NULL,"Text_Analysis3.txt",row.names=F,col.names=F,quote=F,sep=",")   
}


## Store data in HDFS
Sys.setenv(HADOOP_HOME="/hadoop/mr-runtime")
Sys.setenv(HIVE_HOME="/hadoop/hive-runtime")
Sys.setenv(HADOOP_BIN="/hadoop/mr-runtime/bin")
Sys.setenv(HADOOP_CMD ="/hadoop/mr-runtime/bin/hadoop")
require("rhdfs")
hdfs.init()
require(rmr)
hdfs.put("Text_Analysis1.txt",paste("/New_Subro/text_analysis_data/",report,"_1.txt",sep=""),dstFS=hdfs.defaults("fs"))
hdfs.put("Text_Analysis2.txt",paste("/New_Subro/text_analysis_data/",report,"_2.txt",sep=""),dstFS=hdfs.defaults("fs"))
hdfs.put("Text_Analysis3.txt",paste("/New_Subro/text_analysis_data/",report,"_3.txt",sep=""),dstFS=hdfs.defaults("fs"))
unlink("Text_Analysis1.txt")
unlink("Text_Analysis2.txt")
unlink("Text_Analysis3.txt")

content<-hdfs.read.text.file("/New_Subro/input/Subro_Customer_data.txt")
Subro_Customer_data<-read.table(textConnection(content),sep=";",header=T)
ind=which(Subro_Customer_data$Claim.Number==as.numeric(report))

if(length(ind)>0){
Subro_Customer_data[ind,25]=as.numeric(hit_attribution_fault_Measure[2,3])
write.table(Subro_Customer_data,"Subro_Customer_data_new.txt",row.names=F,quote=F,col.names=T,sep=";")
if(hdfs.exists("/New_Subro/input/Subro_Customer_data.txt")==TRUE){
  hdfs.del("/New_Subro/input/Subro_Customer_data.txt")}
hdfs.put("Subro_Customer_data_new.txt","/New_Subro/input/Subro_Customer_data.txt",dstFS=hdfs.defaults("fs"))
unlink("Subro_Customer_data_new.txt")

source("/home/hduser/subro/Subro_Hadoop_V2.R")
}
