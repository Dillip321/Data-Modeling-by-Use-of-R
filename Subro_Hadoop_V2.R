
t0=Sys.time()
#source("/home/shriraj/Codes/Subro_Hadoop.R")

###################################################################################################################################################################################
Sys.setenv(HADOOP_HOME="/hadoop/mr-runtime")
Sys.setenv(HIVE_HOME="/hadoop/hive-runtime")
Sys.setenv(HADOOP_BIN="/hadoop/mr-runtime/bin")
Sys.setenv(HADOOP_CMD ="/hadoop/mr-runtime/bin/hadoop")
require("rhdfs")
hdfs.init()
require(rmr)
require(MASS)
require(epicalc)

#See list of available files from HDFS
#hdfs.ls("/New_Subro")

## Start Reading
#xx=read.csv("Final_Subro_Data.csv")
#write.table(xx,"Final_Subro_Data.txt",row.names=F,quote=F,col.names=T,sep=",")
#hdfs.put("Final_Subro_Data.txt","/New_Subro/input/Subro_data.txt",dstFS=hdfs.defaults("fs"))
content<-hdfs.read.text.file("/New_Subro/input/Subro_data.txt")
subro.data<-read.table(textConnection(content),sep=",",header=T)

# Fit Logistic regression model to find significant factors for cross sell acceptance #
#Logit Model without fault measure
#Logit_Model1<- glm(Subrogation~Age.of.Claimant+Gender+Injury+Jurisdiction.Locale+Loss.Age+Claim.Description.Code+Actual.Claim.Amount,data=subro.data, family=binomial("logit"))

#Logit Model with fault measure
Logit_Model<- glm(Subrogation~Age.of.Claimant+Gender+Injury+Jurisdiction.Locale+Loss.Age+Claim.Description.Code+Actual.Claim.Amount+Fault.Measure,data=subro.data, family=binomial("logit"))


# Converting probabilities
threshold=0.5
fitted=data.frame(fitted(Logit_Model))
fitted$Subrogation=subro.data$Subrogation
fitted$Predicted=NA
fitted[which(fitted[,1]>=threshold),3]=1
fitted[which(fitted[,1]<threshold),3]=0

# 2 X 2 contingency table  
con.mat<-ftable(fitted$Subrogation,fitted$Predicted)
# Add marginal sums #
cont.table<-addmargins(con.mat)
colnames(cont.table)<-c("0","1","Marginal_sum")
rownames(cont.table)<-c("0","1","Marginal_sum")

# Find Model Accuracy #
Accuracy<- ((cont.table[1,1]+cont.table[2,2])/(cont.table[3,3]))*100
Accuracy=data.frame(Accuracy)
Accuracy

###### Second Predictive Model To predict recovery Amount
subro.data2=subro.data[which(subro.data$Subrogation==1),]
#fit.subro2=glm(Recovery.Amount~Age.of.Claimant+Gender+Injury+Jurisdiction.Locale+Claim.Age+Claim.Description.Code+Actual.Claim.Amount,data=subro.data2,family=Gamma("identity"))
GL_Model=glm(Recovery.Amount~Injury+Jurisdiction.Locale+Claim.Description.Code+Actual.Claim.Amount+Fault.Measure,data=subro.data2,family=gaussian(link="identity"))

#Prediction from Model for New data set
content<-hdfs.read.text.file("/New_Subro/input/Subro_Customer_data.txt")
New_data<-read.table(textConnection(content),sep=";",header=T)
RS=New_data$Report.Status
New_data=New_data[,-26]

# Prediction from Logit_Model
Predicted_Probabilities=predict(Logit_Model,newdata=New_data,type="response")
Predicted=data.frame(Predicted_Probabilities,Predicted_Subrogation=NA)
Predicted[which(Predicted[,1]>=threshold),2]=1
Predicted[which(Predicted[,1]<threshold),2]=0
Positive_Subrogation=which(Predicted[,2]==1)

# Prediction from GL_Model
Predicted.Recovery.Amount=predict(GL_Model,newdata=New_data[Positive_Subrogation,],type="respon")
Predicted$Predicted_Subrogation.Opportunity="Poor Subrogation Opportunity"
Predicted$Predicted_Subrogation.Opportunity[Positive_Subrogation]="Good Subrogation Opportunity"
Predicted$Predicted_Recovery.Amount=0
Predicted$Predicted_Recovery.Amount[Positive_Subrogation]=Predicted.Recovery.Amount
New_data=cbind(New_data,Predicted_Probabilities=Predicted$Predicted_Probabilities,
               Predicted_Subrogation=Predicted$Predicted_Subrogation,
               Predicted_Recovery.Amount=Predicted$Predicted_Recovery.Amount,
               Predicted_Subrogation.Opportunity=Predicted$Predicted_Subrogation.Opportunity)

###################
count=NULL
Comment=NULL
var=matrix(data=0,nrow(New_data),ncol=5)
for(i in 1:nrow(New_data))
{
  j=0
  q=NULL
  if(New_data[i,15]>38){j=j+1;q=paste(q,"Age.of.Claimant", sep="; ");var[i,j]="Age.of.Claimant"}
  if(as.character(New_data[i,16])=="Male"){j=j+1;q=paste(q ,"Gender",sep="; ");var[i,j]="Gender"}
  if(as.character(New_data[i,17])=="Yes"){j=j+1;q=paste(q,"Injury",sep="; ");var[i,j]="Injury"}
  if(as.character(New_data[i,18])=="No"){j=j+1;q=paste(q,"Jurisdiction.Locale",sep="; ");var[i,j]="Jurisdiction.Locale"}
  if(New_data[i,23]==1){j=j+1;q=paste(q,"Claim.Description.Code",sep="; ");var[i,j]="Claim.Description.Code"}
  if(j!=0){count[i]=j;Comment[i]=substring(q,2)}else{j=0;q=0}
  if(New_data[i,26]==1){Comment[i]=paste("Our model predicts good subrogation opportunity due to significant impact of",j,"variables namely",Comment[i],sep=" ") 
  }else{
    Comment[i]=paste("Subrogation opportunity is not good enough. Since variables other than",Comment[i],"have less impact on model", sep=" ")}
}
New_data=cbind(New_data,Comments=Comment,Var1=var[,1],Var2=var[,2],Var3=var[,3],Var4=var[,4],Var5=var[,5])

#### Computing Impact of variables
b0=as.numeric(Logit_Model$coefficient[1])
b1=as.numeric(Logit_Model$coefficient[2])
b2=as.numeric(Logit_Model$coefficient[3])
b3=as.numeric(Logit_Model$coefficient[4])
b4=as.numeric(Logit_Model$coefficient[5])
b5=as.numeric(Logit_Model$coefficient[6])
b6=as.numeric(Logit_Model$coefficient[7])
b7=as.numeric(Logit_Model$coefficient[8])
Impact_of_Fault_Measure=Impact_of_Age=Impact_of_Gender=Impact_of_Injury=Impact_of_Jurisdiction_Locale=Impact_of_Loss_Age=Impact_of_Claim_Description=Impact_of_Actual_Claim_Amount=NULL
for(i in 1:nrow(New_data))
{
  p1=1/(1+exp(-b0))
  Impact_of_Age[i]=1/(1+exp(-(b0+b1*New_data$Age.of.Claimant[i])))
  if(as.character(New_data$Gender[i])=="Male")
  {ind1=1
   Impact_of_Gender[i]=1/(1+(exp(-(b0+b1*New_data$Age.of.Claimant[i]+b2*ind1))))
  }else{ind1=0
        Impact_of_Gender[i]=1/(1+(exp(-(b0+b1*New_data$Age.of.Claimant[i]+b2*ind1))))}
  if(as.character(New_data$Injury[i])=="Yes")
  {ind2=1
   Impact_of_Injury[i]=1/(1+(exp(-(b0+b1*New_data$Age.of.Claimant[i]+b2*ind1+b3*ind2))))
  }else{ind2=0
        Impact_of_Injury[i]=1/(1+(exp(-(b0+b1*New_data$Age.of.Claimant[i]+b2*ind1+b3*ind2))))}
  if(as.character(New_data$Jurisdiction.Locale[i])=="Yes")
  {ind3=1
   Impact_of_Jurisdiction_Locale[i]=1/(1+(exp(-(b0+b1*New_data$Age.of.Claimant[i]+b2*ind1+b3*ind2+b4*ind3))))
  }else{ind3=0
        Impact_of_Jurisdiction_Locale[i]=1/(1+(exp(-(b0+b1*New_data$Age.of.Claimant[i]+b2*ind1+b3*ind2+b4*ind3))))}
  Impact_of_Loss_Age[i]=1/(1+(exp(-(b0+b1*New_data$Age.of.Claimant[i]+b2*ind1+b3*ind2+b4*ind3+b5*New_data$Loss.Age[i]))))
  Impact_of_Claim_Description[i]=1/(1+(exp(-(b0+b1*New_data$Age.of.Claimant[i]+b2*ind1+b3*ind2+b4*ind3+b5*New_data$Loss.Age[i]+b6*New_data$Claim.Description.Code[i]))))
  Impact_of_Actual_Claim_Amount[i]=New_data$Predicted_Probabilities[i]
  
  Impact_of_Actual_Claim_Amount[i]=1/(1+(exp(-(b0+b1*New_data$Age.of.Claimant[i]+b2*ind1+b3*ind2+b4*ind3+b5*New_data$Loss.Age[i]+b6*New_data$Claim.Description.Code[i]+b7*New_data$Actual.Claim.Amount[i]))))
  
  Impact_of_Fault_Measure[i]=New_data$Predicted_Probabilities[i]
}
New_data=cbind(New_data,Impact_of_Age,Impact_of_Gender,Impact_of_Injury,Impact_of_Jurisdiction_Locale,Impact_of_Loss_Age,Impact_of_Claim_Description,Impact_of_Actual_Claim_Amount,Impact_of_Fault_Measure)

New_data$RAG="A"
New_data$RAG[New_data$Predicted_Probabilities<((1-.25)*threshold)]="G"
New_data$RAG[New_data$Predicted_Probabilities>((1+.25)*threshold)]="R"
New_data$Predicted_Subrogation_Percentage=sprintf("%.2f",(Predicted_Probabilities*100))
New_data=New_data[,c(1:24,26:42,44:45,25,43)]
New_data$Report.Status=RS

rm(list=setdiff(ls(), c("subro.data","Logit_Model","GL_Model","Predicted","New_data","t0")))

#Remove Output files
if(hdfs.exists("/New_Subro/output/Model_Output.txt")==TRUE){
  hdfs.del("/New_Subro/output/Model_Output.txt")}
if(hdfs.exists("/New_Subro/output/Subro_Customer_data_output.txt")==TRUE){
  hdfs.del("/New_Subro/output/Subro_Customer_data_output.txt")}
# To store output in HDFS
#write.table(New_data,"Subro_Customer_data_output.csv",sep=",")
write.table(New_data,"Subro_Customer_data_output.txt",row.names = FALSE,quote = FALSE,col.names = FALSE,sep=",")
hdfs.put("Subro_Customer_data_output.txt","/New_Subro/output/Subro_Customer_data_output.txt",dstFS=hdfs.defaults("fs"))
unlink("Subro_Customer_data_output.txt")
#content<-hdfs.read.text.file("/Subrogation/output/Subro_Customer_data_output.txt")
#Subro_Customer_data_output<-read.table(textConnection(content),sep=",",header=FALSE)

sink("Model_Output.txt",append = FALSE)
print("Output of Logistic Model")
summary(Logit_Model)
print(" ")
print("##############################################################################################################################################")
print(" ")
print("Output of Gaussian Linear Model")
summary(GL_Model)
print(" ")
sink()
hdfs.put("Model_Output.txt","/New_Subro/output/Model_Output.txt",dstFS=hdfs.defaults("fs"))
unlink("Model_Output.txt")


#content<-hdfs.read.text.file("/Subrogation/output/Model_Output.txt")
#Model_Output<-read.table(textConnection(content),sep=";")

t1=Sys.time()
total_time=t1-t0
print(total_time)



