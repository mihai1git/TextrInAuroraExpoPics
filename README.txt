Proof of Concept for ​Amazon Web Services, Lambda Service

Application business:
Upload scanned promotional documents, from different exhibitions, into Amazon Cloud, S3 service. 
Extract text from scanned documents, and insert it into a database, using Amazon AWS managed services: S3, Textract, Lambda and Aurora.

Using machine learning, the application gains access to the information contained in the documents, gathering information about the companies that promote those documents. 
The intelligent document processing is done using Textract built-in machine learning algorithms for optical character recognition (OCR).

To find insights and relationships in extracted text, another module uses regular expressions. 
The module found in 10 000 extracted lines, around 1300 company info from the following categories: phone, address, mail, site url, contact info, social media url.

Machine learning is a technology that has an increased usability in many companies, where it leads to an increased productivity. 
The application contains one of the top use cases that is used by companies in production environments: extracting and analyzing data from documents.

Components

The application is built from several AWS Services and custom Lambda microservices. In the picture (see ApplicationDescription.html), the components are:

1. Users upload scaned documents as images in the Amazon Cloud, using the Amazon Web Console, in an internet browser

2. Amazon Simple Storage Service (S3) stores the images and notifies Amazon Queue Service 

3. Amazon Queue Service (SQS) manages the S3 messages while sending them to Lambda Service

4. Lambda function that acts like a helper for SQS: com.amazonaws.lambda.mihai.textrinauroraexpopics.handlers.LambdaSQSHelperHandler  

5. Lambda function that notifies AWS Textract Service, receives Textract results and store them in another bucket in S3: com.amazonaws.lambda.mihai.textrinauroraexpopics.handlers.LambdaS3ToTextrHandler; Textract contains Machine Learning algorithms to extract text from images 

6. Amazon Simple Storage Service (S3) stores the result from Textract: extracted text in JSON format 

7. Amazon Simple Notification System (SNS) sends error messages from LambdaS3ToTextrHandler to the administrator

8. Lambda function that transfers JSON files into Amazon Aurora database: com.amazonaws.lambda.mihai.textrinauroraexpopics.handlers.LambdaS3ToDbHandler 

9. Amazon Aurora database: a cloud specific database with MySQL engine and managed by AWS

10. Amazon SNS sends error messages from LambdaS3ToDbHandler to the administrator

11. Virtual Private Cloud (VPC) that manage a subnet with AWS services 

12. VPC Endpoint Gateway that allows communication between the VPC subnet and AWS S3 service.

For more details see ApplicationDescription.html.