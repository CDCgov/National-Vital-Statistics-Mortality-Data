/********************************************************************************************************************
PROGRAM NAME:       MAPPING SEARCH TERMS - MINION
VERSION:			CSP Update 2016 April 01
********************************************************************************************************************/

	/***********************************************************************************************************
	                                                SECTION 1
	************************************************************************************************************/


%put date MAPPING_SEARCH_TERMS_MINION_SIGN_ON START %sysfunc(date(),worddate.);
%put time MAPPING_SEARCH_TERMS_MINION_SIGN_ON START %sysfunc(time(),time8.2);

%include "&ProjectPath.LIBRARIES.sas";
%include "\\cdc\csp_Project\CIPSEA_PII_DVS_MSB\Drug_Involved_Mortality\Final_Programs\2019-01-18\log_csp_signon.sas" /NOSOURCE2 ;
%initialize_signon();

OPTIONS AUTOSIGNON;
%LET RC&sysparm=%SYSFUNC(grdsvc_enable(_ALL_,resource=CSP_MNL )) ;
%PUT Return Code = &&RC&sysparm ;
signon csp&sysparm wait=no MACVAR=SIGNON_RC ;
	%log_signon(session_name=csp&sysparm);

    %SYSLPUT year               		= &year                 		/remote=csp&sysparm;
    %SYSLPUT part               		= &part                 		/remote=csp&sysparm;
    %SYSLPUT firstobs           		= &firstobs             		/remote=csp&sysparm;
    %SYSLPUT obs                		= &obs                  		/remote=csp&sysparm;
    %SYSLPUT input_dataset_folder       = &input_dataset_folder        	/remote=csp&sysparm;
    %SYSLPUT input_dataset_file         = &input_dataset_file    		/remote=csp&sysparm;
    %SYSLPUT input_search_term_dataset  = &input_search_term_dataset	/remote=csp&sysparm;
    %SYSLPUT count_search_term          = &count_search_term        	/remote=csp&sysparm;
    %SYSLPUT search_term_batchsize      = &search_term_batchsize    	/remote=csp&sysparm;
    %SYSLPUT length_search_term         = &length_search_term         	/remote=csp&sysparm;
    %SYSLPUT sysparm         			= &sysparm         				/remote=csp&sysparm;
    %SYSLPUT log_folder  				= &log_folder    				/remote=csp&sysparm;

    rsubmit csp&sysparm wait=no inheritlib=(data input raw results temp);
		proc Printto NEW LOG="&log_folder.MST_MINI_&sysparm..txt";

%put date MAPPING_SEARCH_TERMS_MINION_SIGN_ON DONE  %sysfunc(date(),worddate.);
%put time MAPPING_SEARCH_TERMS_MINION_SIGN_ON DONE %sysfunc(time(),time8.2);

	/***********************************************************************************************************
	                                                SECTION 2
	************************************************************************************************************/

		%macro MAPPING_SEARCH_TERMS_MINION();

		    %put date MAPPING_SEARCH_TERMS_MINION macro started %sysfunc(date(),worddate.);
		    %put time MAPPING_SEARCH_TERMS_MINION macro started %sysfunc(time(),time5.0);

		    %put NOTE: Auto-executable program MST_&sysparm was submitted, importing batch-specific macro variables;

		    %put batch number = &sysparm;

		    %put year                       = &year;
		    %put part                       = &part;
		    %put firstobs                   = &firstobs;
		    %put obs                        = &obs;
		    %put input_dataset_folder       = &input_dataset_folder;
		    %put input_dataset_file         = &input_dataset_file;
		    %put input_search_term_dataset  = &input_search_term_dataset;
		    %put count_search_term          = &count_search_term;
		    %put search_term_batchsize      = &search_term_batchsize;
			%let length_search_term			= %eval(&length_search_term + 1);
		    %put length_search_term         = &length_search_term;

		    proc sql noprint;
		        select strip(upcase(search_term))
					into :search_term_1          - :search_term_%sysfunc(strip(&count_search_term))
					from input.&input_search_term_dataset;
				select countw(search_term)
		            into :countw_search_term_1   - :countw_search_term_%sysfunc(strip(&count_search_term))
		            from input.&input_search_term_dataset;
		    quit;

	/***********************************************************************************************************
	                                                SECTION 3
	************************************************************************************************************/

		    %do search_term_batch = 1 %to %sysfunc(ceil(%sysevalf(&count_search_term / &search_term_batchsize)));
		        %let first_term = %sysevalf((&search_term_batch - 1) * &search_term_batchsize + 1);
		        %let last_term = %sysevalf(&search_term_batch * &search_term_batchsize);
		        %if &last_term > &count_search_term %then %do;
		            %let last_term = &count_search_term;
		        %end;

		        data temp.mentions_&year._&part._&search_term_batch (compress = yes);
		            length  search_term     $&length_search_term..
		                    text_field      $32.
		                    ;
		            set &INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year
		                (firstobs = &firstobs
		                obs = &obs
		                );
		            %do j = 1 %to 3;
		                %if &j=1 %then %do; %let text_field = cleaned_CHAIN;       %end;
		                %if &j=2 %then %do; %let text_field = cleaned_DESCR_LIN5;  %end;
		                %if &j=3 %then %do; %let text_field = cleaned_INJ_DESCR;   %end;
		                %do k = &first_term %to &last_term;
		                    if find(&text_field, "&&search_term_&k") then do position = 1 to (lengthn(&text_field)-lengthn("&&search_term_&k")+1);
		                        if substr(&text_field,position,lengthn("&&search_term_&k")) = "&&search_term_&k" then do;
		                            if position = 1 then do;
		                                if substr(&text_field,position+lengthn("&&search_term_&k"),1) in (" ") then do;
		                                    search_term         = "&&search_term_&k";
		                                    text_field          = upcase("&text_field");
		                                    term_position_beg   = position;
		                                    term_length         = lengthn("&&search_term_&k");
		                                    term_position_end   = term_position_beg + term_length - 1;
		                                    output;
		                                end;
		                                else if substr(&text_field,position+lengthn("&&search_term_&k"),2) in ("S ") then do;
		                                    search_term         = cats("&&search_term_&k","S");
		                                    text_field          = upcase("&text_field");
		                                    term_position_beg   = position;
		                                    term_length         = lengthn("&&search_term_&k") + 1;
		                                    term_position_end   = term_position_beg + term_length - 1;
		                                    output;
		                                end;
		                            end;
		                            else if (substr(&text_field,position-1,1) in (" ")) and
		                                    (substr(&text_field,position+lengthn("&&search_term_&k"),1) in (" ")) then do;
		                                search_term         = "&&search_term_&k";
		                                text_field          = upcase("&text_field");
		                                term_position_beg   = position;
		                                term_length         = lengthn("&&search_term_&k");
		                                term_position_end   = term_position_beg + term_length - 1;
		                                output;
		                            end;
		                            else if (substr(&text_field,position-1,1) in (" ")) and 
		                                    (substr(&text_field,position+lengthn("&&search_term_&k"),2) in ("S ")) then do;
		                                search_term         = cats("&&search_term_&k","S");
		                                text_field          = upcase("&text_field");
		                                term_position_beg   = position;
		                                term_length         = lengthn("&&search_term_&k") + 1;
		                                term_position_end   = term_position_beg + term_length - 1;
		                                output;
		                            end;
		                        end;
		                    end;
		                    position = .;
		                %end;
		                drop position term_length;
		            %end;
		        run;
		    %end;

		    %put date MAPPING_SEARCH_TERMS_MINION macro ended %sysfunc(date(),worddate.);
		    %put time MAPPING_SEARCH_TERMS_MINION macro ended %sysfunc(time(),time5.0);

		%mend;

		%MAPPING_SEARCH_TERMS_MINION();

	endrsubmit;
signoff csp&sysparm;
