/********************************************************************************************************************
PROGRAM NAME:       MAPPING SEARCH TERMS - MINION
VERSION:			CSP Update 2016 April 01
********************************************************************************************************************/

	/***********************************************************************************************************
	                                                SECTION 1
	************************************************************************************************************/

%put date MAPPING_PHRASES_SIGN_ON START %sysfunc(date(),worddate.);
%put time MAPPING_PHRASES_SIGN_ON START %sysfunc(time(),time8.2);

%include "&ProjectPath.LIBRARIES.sas";
%include "\\cdc\csp_Project\CIPSEA_PII_DVS_MSB\Drug_Involved_Mortality\Final_Programs\2019-01-18\log_csp_signon.sas" /NOSOURCE2 ;
%initialize_signon();

OPTIONS AUTOSIGNON;
signon csp&sysparm wait=no MACVAR=SIGNON_RC ;
	%log_signon(session_name=csp&sysparm);

    %SYSLPUT year               		= &year                 	/remote=CSP&sysparm;
    %SYSLPUT part               		= &part                 	/remote=CSP&sysparm;
    %SYSLPUT firstobs           		= &firstobs             	/remote=CSP&sysparm;
    %SYSLPUT obs                		= &obs                  	/remote=CSP&sysparm;
    %SYSLPUT input_dataset_folder       = &input_dataset_folder 	/remote=CSP&sysparm;
    %SYSLPUT input_dataset_file         = &input_dataset_file   	/remote=CSP&sysparm;
    %SYSLPUT input_phrase_list  		= &input_phrase_list		/remote=CSP&sysparm;
    %SYSLPUT unmapped_records_only 		= &unmapped_records_only	/remote=CSP&sysparm;
    %SYSLPUT count_phrase				= &count_phrase		    	/remote=CSP&sysparm;
    %SYSLPUT phrase_batchsize			= &phrase_batchsize			/remote=CSP&sysparm;
    %SYSLPUT length_phrase				= &length_phrase	    	/remote=CSP&sysparm;
    %SYSLPUT sysparm         			= &sysparm         			/remote=csp&sysparm;
    %SYSLPUT log_folder  				= &log_folder    			/remote=CSP&sysparm;

    rsubmit csp&sysparm wait=no inheritlib=(data input raw results temp);
		proc Printto NEW LOG="&log_folder.MP_MINI_&sysparm..txt";

%put date MAPPING_PHRASES_SIGN_ON DONE  %sysfunc(date(),worddate.);
%put time MAPPING_PHRASES_SIGN_ON DONE %sysfunc(time(),time8.2);

	/***********************************************************************************************************
	                                                SECTION 2
	************************************************************************************************************/

		%macro MAPPING_PHRASES_MINION();
			option minoperator mindelimiter=",";
/*			option mprint mlogic;*/

		    %put date MAPPING_PHRASES_MINION macro started %sysfunc(date(),worddate.);
		    %put time MAPPING_PHRASES_MINION macro started %sysfunc(time(),time8.2);

		    %put NOTE: Auto-executable program MP_&sysparm was submitted, importing batch-specific macro variables;

		    %put batch number = &sysparm;

		    %put year                       = &year;
		    %put part                       = &part;
		    %put firstobs                   = &firstobs;
		    %put obs                        = &obs;
		    %put input_dataset_folder       = &input_dataset_folder;
		    %put input_dataset_file         = &input_dataset_file;
		    %put input_phrase_list          = &input_phrase_list;
			%put unmapped_records_only		= &unmapped_records_only;
		    %put count_phrase               = &count_phrase;
		    %put phrase_batchsize           = &phrase_batchsize;
		    %put length_phrase              = &length_phrase;

		    proc sql noprint;
		        select strip(upcase(phrase))
		            into :phrase_1        - :phrase_%sysfunc(strip(&count_phrase))
		            from input.phrases
		            %if &input_phrase_list ~= %then %do;
		                where phrase_list = &input_phrase_list
		            %end;
		            ;
				%let first_phrase = 1;
		        select count(phrase)
		            into :last_phrase
		            from input.phrases
		            %if &input_phrase_list ~= %then %do;
		                where phrase_list = &input_phrase_list
		            %end;
		            ;
		        select countw(phrase)
		            into :countw_phrase_1 - :countw_phrase_%sysfunc(strip(&count_phrase))
		            from input.phrases
		            %if &input_phrase_list ~= %then %do;
		                where phrase_list = &input_phrase_list
		            %end;
		            ;
		    quit;

	/***********************************************************************************************************
	                                                SECTION 3
	************************************************************************************************************/

		    %do phrase_batch = 1 %to %sysfunc(ceil(%sysevalf(&count_phrase / &phrase_batchsize)));
/*		        %let first_phrase = %sysevalf((&phrase_batch - 1) * &phrase_batchsize + 1);*/
/*		        %let last_phrase = %sysevalf(&phrase_batch * &phrase_batchsize);*/
/*		        %if &last_phrase > &count_phrase %then %do;*/
/*		            %let last_phrase = &count_phrase;*/
/*		        %end;*/

		        data temp.phrase_mentions_&year._&part._&phrase_batch (compress = yes);
		            length  phrase  $&length_phrase..;
		            set &INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year
		                (firstobs = &firstobs
		                obs = &obs
		                );
		            %if &INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year in (data.phrase_mentions_&year, temp.phrase_mentions_&year) %then %do;
		                if strip(phrase) = "*" then do;
		            %end;
						%if &UNMAPPED_RECORDS_ONLY = 1 %then %do;
							if strip(phrase) in ("*","") then do;
						%end;
						if strip(phrase) not in ("*","") then output;
		            %do k = &first_phrase %to &last_phrase;
		                if find(distilled_literal, "&&phrase_&k") 
		                then do position = 1 to (lengthn(distilled_literal) - lengthn("&&phrase_&k") + 1);
		                    if substr(distilled_literal,position,lengthn("&&phrase_&k"))="&&phrase_&k" then do;
		                        if position = 1 then do;
		                            if substr(distilled_literal,position+lengthn("&&phrase_&k"),1) in (" ") then do;
		                                phrase              = "&&phrase_&k"         ;
		                                phrase_beg          = position;
		                                phrase_length       = lengthn("&&phrase_&k");
		                                phrase_end          = phrase_beg + phrase_length - 1;
		                                if phrase_beg <= distilled_position <= phrase_end then do;
											phrase_list = "&input_phrase_list";
											output;
										end;
		                            end;
		                        end;
		                        else if substr(distilled_literal,position - 1,1) in (" ") then do;
		                            if substr(distilled_literal,position+lengthn("&&phrase_&k"),1) in (" ") then do;
		                                phrase              = "&&phrase_&k"         ;
		                                phrase_beg          = position;
		                                phrase_length       = lengthn("&&phrase_&k");
		                                phrase_end          = phrase_beg + phrase_length - 1;
		                                if phrase_beg <= distilled_position <= phrase_end then do;
											phrase_list = "&input_phrase_list";
											output;
										end;
		                            end;
		                        end;
		                    end;
		                end;
		                position = .;
		            %end;
		            %if &INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year in (data.phrase_mentions_&year, temp.phrase_mentions_&year) %then %do;
		                end;
		                else output;
		            %end;
						%if &UNMAPPED_RECORDS_ONLY = 1 %then %do;
			                end;
			                else output;
						%end;
		            drop phrase_length position;
		        run;
		    %end;

		    %put date MAPPING_PHRASES_MINION macro ended %sysfunc(date(),worddate.);
		    %put time MAPPING_PHRASES_MINION macro ended %sysfunc(time(),time8.2);
		%mend MAPPING_PHRASES_MINION;

		%MAPPING_PHRASES_MINION;

	endrsubmit;
signoff csp&sysparm;


