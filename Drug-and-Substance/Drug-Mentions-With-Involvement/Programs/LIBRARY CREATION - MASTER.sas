/********************************************************************************************************************
PROGRAM NAME:                   LIBRARY CREATION
VERSION:						CSP Update 2016 April 01

PURPOSE:                        To create the LIBRARIES program, which is called in USER INPUT and other programs
        
INPUT DATASETS:                 None

INPUT GLOBAL MACRO VARIABLES:   None

BATCH PROCESSING:               None

MACROS EMBEDDED:                LIBRARY_CREATION (see USER INPUT program for additional details)

OUTPUT DATASETS AND VARIABLES:  None
********************************************************************************************************************/

%macro library_creation(raw_folderpath,
						input_folderpath,
                        data_folderpath,
                        results_folderpath,
                        temp_folderpath,
						log_folderpath,
						autoexec_folderpath
                        );

	%let projectpath = %sysfunc(compress(&projectpath))		;
	%let raw_folderpath 	= %sysfunc(compress(&raw_folderpath))			;

    filename LIBRARY "&projectpath.LIBRARIES.sas";

    data _null_;
        file LIBRARY;
        put "options dlcreatedir;";
        put "libname raw        %sysfunc(compress('&raw_folderpath'));"		;
        put "libname input      %sysfunc(compress('&input_folderpath'));"	;
        put "libname data       %sysfunc(compress('&data_folderpath'));"	;
        put "libname results    %sysfunc(compress('&results_folderpath'));"	;
        put "libname temp       %sysfunc(compress('&temp_folderpath'));"	;
        put "libname log        %sysfunc(compress('&log_folderpath'));"		;
        put "libname autoexec   %sysfunc(compress('&autoexec_folderpath'));"		;
    run;
    quit;
%mend;
