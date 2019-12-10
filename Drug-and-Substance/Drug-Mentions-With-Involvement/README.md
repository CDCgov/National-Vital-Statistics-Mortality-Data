

# Drug Mentions with Involvement (DMI) Methodology 
Published article: Using Literal Text From the Death Certificate to Enhance Mortality Statistics: Characterizing Drug Involvement in Deaths. 

Other documentation: Information on the Drug-Involved Mortality Restricted Data, Research Data Center, National Center for Health Statistics. 

## Overview 


This GitHub repository contains a suite of programs that allow for obtaining information on substances, including prescription and illicit drugs, mentioned on death certificate records as having been involved in deaths of U.S. residents of the 50 states and District of Columbia.  The methods are often referred to as Drug Mentions with Involvement (DMI), and the data created from these programs are referred as the Drug-Involved Mortality restricted-use data. Restricted-use data can be requested via the Research Data Center at the National Center for Health Statistics. 


These programs were created for use by CDC programs to collaborate on public health surveillance related projects in support of the CDC Surveillance Strategy. This third party web application is not hosted by the CDC, but is used by CDC and its partners to share information and collaborate on software. 


## General Requirements 


These programs were prepared using the Microsoft Windows 10 operating system. Software necessary to utilize these programs are: 1) Microsoft Excel, and 2) SAS Foundation or Base SAS. 


## Directory Structure  


- Programs Folder contains SAS code for searching the literal text for drugs/substances: 


    - LIBRARY CREATION - MASTER.sas – SAS code that reads folder locations and creates a libraries.sas program. 


    - LIBRARIES.sas – SAS code that specifies the source folder locations 


    - POPULATION SELECTION - MASTER.sas – SAS code that subsets data 


    - log_csp_signon.sas – SAS program for signing onto a server in preparation for executing multiple threads. 


    - RECORD SELECTION - MASTER.sas - Sets up parallel processing threads for record selection (for large data files).  


    - RECORD SELECTION - MINION.sas - Selects records based on underlying or multiple cause of death. Also cleans the literal text for symbols, white space, etc. 


    - MAPPING SEARCH TERMS - MASTER.sas - Sets up parallel processing threads for searching for specific search terms. 


    - MAPPING SEARCH TERMS - MINION.sas - Locates search terms within literal text. 


    - MAPPING QUALIFIERS - MASTER.sas - Sets up parallel processing threads to search for qualifying phrases i.e., – prescription, recreational, etc. 


    - MAPPING QUALIFIERS - MINION.sas - Locates qualifiers within literal text. 


    - DISTILLING LITERALS - MASTER.sas - Replaces search term and various qualifiers with ‘*’ reducing the complexity of the text the phrase search program must execute on. 


    - MAPPING PHRASES - MASTER.sas - Sets up parallel processing threads to search for specific phrases. 


    - MAPPING PHRASES - MINION.sas - Searches the literal text for specific phrases. 


    - ANALYSIS PREPARATION - MASTER.sas - Outputs a mention level file with all found search terms, qualifiers, and phrases. 



- Documentation – identifies changes in the DMI programs made over time by the National Center for Health Statistics 


- User Interface.xlsm – is a master Microsoft Excel (.xlsm) file for running the SAS programs (listed in the program folder) that enable the user to provide input on parameters of the text search. For instance, you can establish additional drug terms to search beyond the original pre-defined list. 


    - User_Interface - Contains visual basic code that allows provide data to and execute the DMI SAS programs.   


    - Acceptable_Values - Identifies what input is acceptable for the User_Interface tab. 


    - icd_criteria_sas7bdat - list of ICD-10 codes to filter input death records to drugs considered involved in the death 


    - Terms Categories and Definitions - code book for search term tab columns. 


    - Updated_Terms (20190118) - list of drug names to search against the literal text 


     - Qualifiers.sas7bdat - terms to search against the literal text appearing next to the found drug mentions 


    - EXCLUDE_SEARCH_TERMS – list of search terms to exclude from analysis. 


    - Descriptor Descriptions – code book for descriptor tab columns 


    - Updated_Descript (20190118) - list of descriptors 


    - EXCLUDE_DESCRIPTORS – list of descriptors to search for, but exclude. 


    - Updated_Joining (20190118) —joining phrases to search against the literal text that link the found drug mentions 


    - Phrase Descriptions  – code book for phrase tab columns 


    - Updated_Phrases (20190118) —phrases to search against the literal text that contain the found drug mention with qualifiers and joining phrases 


    - EXCLUDE_PHRASES – list of phrases to search for but exclude from analysis. 



    - Additional Text Instructions – details for Contributing, Disclaimer, Licence, and Code of Conduct 


 


 


## Set-up Instructions 


These files are currently provided ‘as is’. Code has been canned for logging onto and running selected portions of these programs in parallel on the National Vital Statistics System servers. This code must be commented out or edited for running in your specific environment. The following instructions should be followed in general, after editing the programs to run in your environment. 


1)  Inspect the SAS files and save them in a designated location on your computer.  



1)  Take note that all SAS programs are run from an enclosed Excel file (i.e., “user_interface.xlsm”), which is intended to clearly delineate required user input and execution scripts (i.e. buttons). Within the “user_interface” file, you can establish additional drug terms to search. 



1)  Save the ‘User Interface.xlsm’ excel file to the folder from which you would like to run your analyses from. 



1)  In the User_Interface tab of the ‘User Interface.xlsm’ excel file click into cell D2. A browse box will appear asking for a folder selection. Select the folder that you saved the ‘User Interface.xlsm’ excel file. 



1)  Click cell D3 to similarly source the folder where you saved the SAS programs. 



1)  Then click the button in cells A5-A12 titled ‘Press to create library locations’. This will create all subfolders in cells D5-D12 in the same project folder that the ‘User Interface.xlsm’ excel file was saved. 



1)  After checking to make sure that the name within each cell from D14-D19 exactly matches the name of the coresponding tab within the ‘User Interface.xlsm’ excel file, click the button in cells A14-A19 titled ‘Press to import input data into SAS’. 



1)  Data is processed year by year. Edit cells D21 and D22 so that the start year and end year match your requirements. 



1)  Cell D24 is the name of the input files in the form ‘Your input data file name####’ where ‘####’ is the full four digit year the data comes from (i.e. 2003). 



1)  If cell D27 is populated a SAS program will subset the data according to the information in this cell. Currently the cell subsets on state of residence as found in historical mortality files. 



1)  Click the button in cells A24-A28 to execute the SAS program to subset records, in this case on state of residence if D27 is populated. 



1)  Review each title in cells B30-B38 and then edit the data in cells D30-D38 according to your projects requirements. In particular cells B32 and B33 must be populated. The number of records that each thread will process and the total number of threads the program will allow at once are required. All other cells are optional and are for specific types of analyses D34-D38 or to subset the data for running tests using D30 and D31. 



1)  Review each title in cells B40-B49 and then edit the data in cells D40-D49 according to your projects requirements. In particular cells B40-B42 and B45-B48 must be populated. Typically the Data folder will be input dataset folder and the output dataset folder in cells D40 and D42 respectively. In addition the name of the file to process in this step will be ‘cleaned_records’. The name of the ‘Input_search_term_Dataset’ cell D45 will usually be ‘GENERAL_SEARCH_TERMS’ or ‘USER_DEFINED_SEARCH_TERMS’. Again the number of records that each thread will process and the total number of threads the program will allow at once are required in cells D46 and D47. Cell D48 is a required field and will run a small program for searching a the literal text fields by a subset of the full search term list. The number placed in cell D48 will determine how many search terms will searched at one time. All other cells are optional and are for specific types of analyses D43, D44, and D49. 



1)  The Button in cells A51 and A52 is for identifying text which overlaps between two other search terms and has been found to be very helpful for identifying additional search terms. 



1)  At this point the user can stop executing any additional visual basic on this User_Interface tab if all that is needed is to search for drug and/or substances. To continue on is to also identify phrases which can be helpful for identifying a negating context or to identify how a drug/substance was used. 



1)  To search for qualifiers in the literal text the cells D55 and D56 must be filled and again are the number of records to search and the number of max threads to use. Press the button labeled ‘Press to map qualifiers’ in cells A55-A58 when ready. 



1)  No additional input is required to prepare the data for searching for phrases. Just press the button titled ‘Press to distill literals’. 



1)  The button titled ‘Press to map phrases’ in cells A66-A74 must be clicked at least 6 times for each dataset. Between each iteration of the map phrases button the cell D69 must be changed sequentially from 1 to 6. In addition after the first iteration the cell D67 must be changed from ‘distilled_literals’ to ‘phrase_mentions’. Cells D66 and D68 should usually be filled by ‘data’ and must be populated. Keeping cell D70, unmapped_record_only, as ‘1’ will force the program to reevaluate a phrase on each of the six iterations. If phrase_batch_size is populated then a subset of the phrases will be searched for. ‘Batch_redux’ in cell D74 is for researching a part of the dataset. 



1)  Cells A76-A78 is for identifying new phrases for when predefined phrases overlap and it would be instructive to create one new phrase. 



1)  To recreate result datasets press the button in cells A80-A82. If cell D80 contains ‘1’ then only records where a search term was found will be in the output dataset. If cell D81 contains ‘1’ then all addtional columns from the original dataset will be included in the output. 



## Public Domain 


This project constitutes a work of the United States Government and is not subject to domestic copyright protection under 17 USC § 105. This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the CC0 1.0 Universal public domain dedication. All contributions to this project will be released under the CC0 dedication. By submitting a pull request you are agreeing to comply with this waiver of copyright interest. 


##  License 


The project utilizes code licensed under the terms of the Apache Software License and therefore is licensed under ASL v2 or later. 


This program is free software: you can redistribute it and/or modify it under the terms of the Apache Software License version 2, or (at your option) any later version. 


This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the Apache Software License for more details. 


You should have received a copy of the Apache Software License along with this program. If not, see http://www.apache.org/licenses/LICENSE-2.0.html 


##  Privacy 


This project contains only non-sensitive, publicly available data and information. All material and community participation is covered by the Surveillance Platform Disclaimer and Code of Conduct. For more information about CDC's privacy policy, please visit http://www.cdc.gov/privacy.html. 


##  Contributing 


Anyone is encouraged to contribute to the project by forking and submitting a pull request. (If you are new to GitHub, you might start with a basic tutorial.) By contributing to this project, you grant a world-wide, royalty-free, perpetual, irrevocable, non-exclusive, transferable license to all users under the terms of the Apache Software License v2 or later. 


All comments, messages, pull requests, and other submissions received through CDC including this GitHub page are subject to the Presidential Records Act and may be archived. Learn more at http://www.cdc.gov/other/privacy.html. 


##  Records 


This project is not a source of government records, but is a copy to increase collaboration and collaborative potential. All government records will be published through the CDC web site. 


##  Notices 


Please refer to CDC's Template Repository for more information about contributing to this repository, public domain notices and disclaimers, and code of conduct. 


##  Hat-tips 


Thanks to 18F's open source policy and code of conduct that were very useful in setting up this GitHub organization. Thanks to CDC's Informatics Innovation Unit that was helpful in modeling the code of conduct. 


 
 			
