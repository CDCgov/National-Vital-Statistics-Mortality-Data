/********************************************************************************************************************
PROGRAM NAME:       RECORD SELECTION - MINION
VERSION:			CSP Update 2016 April 01
********************************************************************************************************************/


	/***********************************************************************************************************
	                                                SECTION 1
	************************************************************************************************************/

%put date RECORD_SELECTION_SIGN_ON START %sysfunc(date(),worddate.);
%put time RECORD_SELECTION_SIGN_ON START %sysfunc(time(),time8.2);

%include "&ProjectPath.LIBRARIES.sas";
%include "\\cdc\csp_Project\CIPSEA_PII_DVS_MSB\Drug_Involved_Mortality\Final_Programs\2019-01-18\log_csp_signon.sas" /NOSOURCE2 ;
%initialize_signon();

OPTIONS AUTOSIGNON;
%LET RC&sysparm=%SYSFUNC(grdsvc_enable(_ALL_,resource=CSP_MNL )) ;
%PUT Return Code = &&RC&sysparm ;
signon csp&sysparm wait=no MACVAR=SIGNON_RC ;
	%log_signon(session_name=csp&sysparm);

	%SYSLPUT year               = &year                 /remote=csp&sysparm;
	%SYSLPUT part               = &part                 /remote=csp&sysparm;
	%SYSLPUT firstobs           = &firstobs             /remote=csp&sysparm;
	%SYSLPUT obs                = &obs                  /remote=csp&sysparm;
	%SYSLPUT select_on_ICD      = &select_on_ICD        /remote=csp&sysparm;
	%SYSLPUT systematic_sample  = &systematic_sample    /remote=csp&sysparm;
	%SYSLPUT sample_shift       = &sample_shift         /remote=csp&sysparm;
	%SYSLPUT sysparm  			= &sysparm	    		/remote=csp&sysparm;
	%SYSLPUT log_folder  		= &log_folder    		/remote=csp&sysparm;

    rsubmit csp&sysparm wait=no inheritlib=(data input raw results temp);
		proc Printto NEW LOG="&log_folder.RS_MINI_&sysparm..txt";

%put date RECORD_SELECTION_SIGN_ON DONE %sysfunc(date(),worddate.);
%put time RECORD_SELECTION_SIGN_ON DONE %sysfunc(time(),time8.2);

	/***********************************************************************************************************
	                                                SECTION 2
	************************************************************************************************************/

		%macro RECORD_SELECTION_MINION();

		    option minoperator mprint mlogic mindelimiter=",";

		    %put date RECORD_SELECTION_MINION macro started %sysfunc(date(),worddate.);
		    %put time RECORD_SELECTION_MINION macro started %sysfunc(time(),time8.2);

		    %put NOTE: Auto-executable program RS_&sysparm was submitted, importing batch-specific macro variables;

		    %put batch number = &sysparm;
		    %put year               = &year;
		    %put part               = &part;
		    %put firstobs           = &firstobs;
		    %put obs                = &obs;
		    %put select_on_ICD      = &select_on_ICD;
		    %put systematic_sample  = &systematic_sample;
		    %put sample_shift       = &sample_shift;

	/***********************************************************************************************************
	                                                SECTION 3
	************************************************************************************************************/

		    proc sql noprint;
		        create table temp.selected_records_&year._&part
		            (compress = yes 
		            drop = record_count) 
		            as
		            select  monotonic() as record_count,
		                    uniq_id, 
		                    DESCR_LIN1, 
		                    DESCR_LIN2, 
		                    DESCR_LIN3, 
		                    DESCR_LIN4,
		                    DESCR_LIN5, 
		                    INJ_DESCR
		                from data.selected_population_&year 
		                    (firstobs=&firstobs
		                    obs = &obs
		                    )               
		                %if &SELECT_ON_ICD>0 %then %do; where
		                    %if (&SELECT_ON_ICD = 1 or &SELECT_ON_ICD = 5 or &SELECT_ON_ICD = 6 or &SELECT_ON_ICD = 7 or &SELECT_ON_ICD = 8 or &SELECT_ON_ICD = 9 or &SELECT_ON_ICD = 10) %then %do;
		                         strip(upcase(CAUSE)) in
		                            (select distinct strip(upcase(ICD_UCD))
		                                from input.ICD_CRITERIA
		                                where strip(ICD_UCD)~=""
		                            )
		                    %end;
		                    %if (&SELECT_ON_ICD = 5 or &SELECT_ON_ICD = 7 or &SELECT_ON_ICD = 9) %then %do;
		                        and (
		                    %end;
		                    %if (&SELECT_ON_ICD = 6 or &SELECT_ON_ICD = 8 or &SELECT_ON_ICD = 10) %then %do;
		                        or
		                    %end;
		                    %if (&SELECT_ON_ICD = 2 or &SELECT_ON_ICD = 3 or &SELECT_ON_ICD = 4 or &SELECT_ON_ICD = 5 or &SELECT_ON_ICD = 6 or &SELECT_ON_ICD = 7 or &SELECT_ON_ICD = 8 or &SELECT_ON_ICD = 9 or &SELECT_ON_ICD = 10) %then %do;
		                        %if (&SELECT_ON_ICD = 2 or &SELECT_ON_ICD = 4 or &SELECT_ON_ICD = 5 or &SELECT_ON_ICD = 6 or &SELECT_ON_ICD = 9 or &SELECT_ON_ICD = 10)  %then %do;
		                            strip(upcase(substr(ENTAX1,3))) in
		                                (select distinct strip(upcase(ICD_MMCD))
		                                    from input.ICD_CRITERIA
		                                    where strip(ICD_MMCD)~=""
		                                )
		                            %do i=2 %to 20;
		                                or strip(upcase(substr(ENTAX&i,3))) in
		                                    (select distinct strip(upcase(ICD_MMCD))
		                                        from input.ICD_CRITERIA
		                                        where strip(ICD_MMCD)~=""
		                                )
		                            %end;
		                        %end;
		                        %if (&SELECT_ON_ICD = 4 or &SELECT_ON_ICD = 9 or &SELECT_ON_ICD = 10) %then %do;
		                            or 
		                        %end;
		                        %if (&SELECT_ON_ICD = 3 or &SELECT_ON_ICD = 4 or &SELECT_ON_ICD = 7 or &SELECT_ON_ICD = 8 or &SELECT_ON_ICD = 9 or &SELECT_ON_ICD = 10) %then %do;
		                            strip(upcase(RECAX1)) in
		                                (select distinct strip(upcase(ICD_MMCD))
		                                    from input.ICD_CRITERIA
		                                    where strip(ICD_MMCD)~=""
		                                )
		                            %do i=2 %to 20;
		                                or strip(upcase(RECAX&i)) in
		                                    (select distinct strip(upcase(ICD_MMCD))
		                                        from input.ICD_CRITERIA
		                                        where strip(ICD_MMCD)~=""
		                                    )
		                            %end;
		                        %end;
		                    %end;
		                    %if (&SELECT_ON_ICD = 5 or &SELECT_ON_ICD = 7 or &SELECT_ON_ICD = 9) %then %do;
		                        )
		                    %end;
		                    %if (&SELECT_ON_ICD = 11 or &SELECT_ON_ICD = 13) %then %do;
		                        catx(" ",strip(upcase(CAUSE)),strip(upcase(substr(ENTAX1,3)))) in
		                            (select distinct catx(" ",strip(upcase(ICD_UCD)),strip(upcase(ICD_MMCD)))
		                                from input.ICD_CRITERIA
		                                where strip(ICD_UCD)~="" and strip(ICD_MMCD)~=""
		                            )
		                        %do i=2 %to 20;
		                            or catx(" ",strip(upcase(CAUSE)),strip(upcase(substr(ENTAX&i,3)))) in
		                                (select distinct catx(" ",strip(upcase(ICD_UCD)),strip(upcase(ICD_MMCD)))
		                                    from input.ICD_CRITERIA
		                                    where strip(ICD_UCD)~="" and strip(ICD_MMCD)~=""
		                                )
		                        %end;
		                    %end;
		                    %if &SELECT_ON_ICD = 13 %then %do;
		                        or
		                    %end;
		                    %if (&SELECT_ON_ICD = 12 or &SELECT_ON_ICD = 13) %then %do;
		                        catx(" ",strip(upcase(CAUSE)),strip(upcase(RECAX1))) in
		                            (select distinct catx(" ",strip(upcase(ICD_UCD)),strip(upcase(ICD_MMCD)))
		                                from input.ICD_CRITERIA
		                                where strip(ICD_UCD)~="" and strip(ICD_MMCD)~=""
		                            )
		                        %do i=2 %to 20;
		                            or catx(" ",strip(upcase(CAUSE)),strip(upcase(RECAX&i))) in
		                                (select distinct catx(" ",strip(upcase(ICD_UCD)),strip(upcase(ICD_MMCD)))
		                                    from input.ICD_CRITERIA
		                                    where strip(ICD_UCD)~="" and strip(ICD_MMCD)~=""
		                                )
		                        %end;
		                    %end;
		                %end;
		                %if &SYSTEMATIC_SAMPLE>0 %then %do;
		                    having (record_count - &sample_shift)/&SYSTEMATIC_SAMPLE = intz(record_count/&SYSTEMATIC_SAMPLE);
		                %end;
		            ;
		    quit;

	/***********************************************************************************************************
	                                                SECTION 4
	************************************************************************************************************/

			data temp.cleaned_records_&year._&part
			    (compress = yes
			    drop = DESCR_LIN1 DESCR_LIN2 DESCR_LIN3 DESCR_LIN4);
			    length  chain $490.             /*Each DESCR_LIN# is 120 characters, plus 3x " | ", plus one buffer character when concatenated*/
			            cleaned_chain $490.;    /*Each DESCR_LIN# is 120 characters, plus 3x " | ", plus one buffer character when concatenated*/
			    set temp.selected_records_&year._&part;
			    chain               = catx(" | ", DESCR_LIN1, DESCR_LIN2, DESCR_LIN3, DESCR_LIN4)||" ";
			    cleaned_CHAIN       = upcase(chain);
			    cleaned_DESCR_LIN5  = upcase(DESCR_LIN5)||" ";
			    cleaned_INJ_DESCR   = upcase(INJ_DESCR)||" ";
			        
			    %do i=1 %to 3;
					%put i = &i;
			        %if &i = 1 %then %do; %let literal = cleaned_chain; %end;
			        %if &i = 2 %then %do; %let literal = cleaned_DESCR_LIN5; %end;
			        %if &i = 3 %then %do; %let literal = cleaned_INJ_DESCR; %end;
			        &literal=strip(&literal);
			        &literal=tranwrd(&literal,"0"," ");
			        &literal=tranwrd(&literal,"1"," ");
			        &literal=tranwrd(&literal,"2"," ");
			        &literal=tranwrd(&literal,"3"," ");
			        &literal=tranwrd(&literal,"4"," ");
			        &literal=tranwrd(&literal,"5"," ");
			        &literal=tranwrd(&literal,"6"," ");
			        &literal=tranwrd(&literal,"7"," ");
			        &literal=tranwrd(&literal,"8"," ");
			        &literal=tranwrd(&literal,"9"," ");
			        &literal=tranwrd(&literal,"N/A"," ");
			        &literal=tranwrd(&literal,"BLANK"," ");
			        &literal=tranwrd(&literal,"`"," ");
			        &literal=tranwrd(&literal,"~"," ");
			        &literal=tranwrd(&literal,"!"," ");
			        &literal=tranwrd(&literal,"@"," ");
			        &literal=tranwrd(&literal,"#"," ");
			        &literal=tranwrd(&literal,"$"," ");
			        &literal=tranwrd(&literal,"%"," ");
			        &literal=tranwrd(&literal,"^"," ");
			        &literal=tranwrd(&literal,"&"," ");
			        &literal=tranwrd(&literal,"*"," ");
			        &literal=tranwrd(&literal,"_"," ");
			        &literal=tranwrd(&literal,"-"," ");
			        &literal=tranwrd(&literal,"+"," ");
			        &literal=tranwrd(&literal,"="," ");
			        &literal=tranwrd(&literal,"{"," ");
			        &literal=tranwrd(&literal,"}"," ");
			        &literal=tranwrd(&literal,"["," ");
			        &literal=tranwrd(&literal,"]"," ");
			        &literal=tranwrd(&literal,"|"," ");
			        &literal=tranwrd(&literal,"\"," ");
			        &literal=tranwrd(&literal,":"," ");
			        &literal=tranwrd(&literal,";"," ");
			        &literal=tranwrd(&literal,"<"," ");
			        &literal=tranwrd(&literal,">"," ");
			        &literal=tranwrd(&literal,","," ");
			        &literal=tranwrd(&literal,"."," ");
			        &literal=tranwrd(&literal,"?"," ");
			        &literal=tranwrd(&literal,"/"," ");
			        &literal=tranwrd(&literal,"("," "); 
			        &literal=tranwrd(&literal,")"," "); 
					&literal=tranwrd(&literal,'"'," ");
					&literal=tranwrd(&literal,"'"," ");
			        %do j=1 %to 5;
			            &literal=tranwrd(&literal,"  "," ");
			        %end;
			    %end;
			run;

			%put date RECORD_SELECTION_MINION macro ended %sysfunc(date(),worddate.);
			%put time RECORD_SELECTION_MINION macro ended %sysfunc(time(),time8.2);
		%mend RECORD_SELECTION_MINION;

		%RECORD_SELECTION_MINION;

	endrsubmit;
signoff csp&sysparm;


