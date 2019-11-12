/********************************************************************************************************************
PROGRAM NAME:       MAPPING QUALIFIERS - MINION
VERSION:			CSP Update 2016 April 01
********************************************************************************************************************/

	/***********************************************************************************************************
	                                                SECTION 1
	************************************************************************************************************/


%put date MAPPING_QUALIFIERS_MINION_SIGN_ON START %sysfunc(date(),worddate.);
%put time MAPPING_QUALIFIERS_MINION_SIGN_ON START %sysfunc(time(),time8.2);

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
    %SYSLPUT count_qualifiers           = &count_qualifiers        	    /remote=csp&sysparm;
    %SYSLPUT sysparm         			= &sysparm         				/remote=csp&sysparm;
    %SYSLPUT log_folder  				= &log_folder    				/remote=csp&sysparm;

	rsubmit csp&sysparm wait=no inheritlib=(data input raw results temp);
		proc Printto NEW LOG="&log_folder.MQ_MINI_&sysparm..txt";

%put date MAPPING_QUALIFIERS_MINION_SIGN_ON DONE %sysfunc(date(),worddate.);
%put time MAPPING_QUALIFIERS_MINION_SIGN_ON DONE %sysfunc(time(),time8.2);

	/***********************************************************************************************************
	                                                SECTION 2
	************************************************************************************************************/

		%macro MAPPING_QUALIFIERS_MINION();

		    %put date MAPPING_QUALIFIERS_MINION macro started %sysfunc(date(),worddate.);
		    %put time MAPPING_QUALIFIERS_MINION macro started %sysfunc(time(),time5.0);

		    %put NOTE: Auto-executable program MQ_&sysparm was submitted, importing batch-specific macro variables;

		    %put batch number = &sysparm;

		    %put year                       = &year;
		    %put part                       = &part;
		    %put firstobs                   = &firstobs;
		    %put obs                        = &obs;
		    %put count_qualifiers           = &count_qualifiers;

		    proc sql noprint;
		        select  strip(upcase(pre_qualifier)),
		                strip(upcase(post_qualifier))
		                into 
		                :pre_qualifier_1    - :pre_qualifier_&count_qualifiers,
		                :post_qualifier_1   - :post_qualifier_&count_qualifiers
		            from input.qualifiers
					where pre_qualifier ~= "" or post_qualifier ~= "";
		    quit;

			%let DSID = %sysfunc(open(input.joining_phrases, IS));
			%if &DSID ~= 0 %then %do;
			    %let anobs = %sysfunc(attrn(&DSID, ANOBS));
			    %let whstmt = %sysfunc(attrn(&DSID, WHSTMT));
			    %if &anobs = 1 & &whstmt = 0 %then %let count_joining = %sysfunc(attrn(&DSID, NLOBS));
			    %let DSID = %sysfunc(close(1));
			    %if &count_joining>0 %then %do;
			        proc sql noprint;
			            select  tranwrd(strip(upcase(joining_phrase)), "*", "")
			                    into :joining_phrase_1 - :joining_phrase_&count_joining
			                    from input.joining_phrases;
			            select  lengthn(strip(joining_phrase))
			                    into
			                    :joining_length_1 - :joining_length_&count_joining
			                from input.joining_phrases;
			        quit;
			    %end;
			%end;
			%else %do;
			    %let counted = 0;
			%end;

	/***********************************************************************************************************
	                                                SECTION 3
	************************************************************************************************************/

			data temp.qualified_mentions_&year._&part (compress = yes);
			    set data.mentions_&year
					(firstobs = &firstobs
					obs = &obs
					);
			    length  pre_qualifier $100.
			            post_qualifier $100.
			            qualified_term $100.
			    ;
			    %do j = 1 %to 3;
			        %if &j=1 %then %do; %let text_field = CLEANED_CHAIN;         %end;
			        %if &j=2 %then %do; %let text_field = CLEANED_DESCR_LIN5;    %end;
			        %if &j=3 %then %do; %let text_field = CLEANED_INJ_DESCR;     %end;

			        if text_field = "&text_field" then do;
			            position = term_position_beg;
			            do K = 1 to &count_qualifiers;
			                if symget('pre_qualifier_'||left(K)) ~= "" then do;
			                    if term_position_beg ~= position then do c = 1 to &count_joining;
			                        if symget('joining_phrase_'||left(c)) ~= "" then do;
			                            if K ~= 0 then do;
			                                if lengthn(symget('pre_qualifier_'||left(K))) + 1 + lengthn(symget('joining_phrase_'||left(c))) < (position - 1) then do;
			                                    if substr(  &text_field,
			                                                position - lengthn(symget('pre_qualifier_'||left(K))) - lengthn(symget('joining_phrase_'||left(c))) - 2,
			                                                lengthn(symget('pre_qualifier_'||left(K))) + lengthn(symget('joining_phrase_'||left(c))) + 1 
			                                                ) = catx(" ", symget('pre_qualifier_'||left(K)), symget('joining_phrase_'||left(c))) then do;
			                                        if position - lengthn(symget('pre_qualifier_'||left(K))) - lengthn(symget('joining_phrase_'||left(c))) - 3 = 0 then do;
			                                            pre_qualifier       = catx(" ", symget('pre_qualifier_'||left(K)), symget('joining_phrase_'||left(c)), pre_qualifier);
			                                            position            = position - lengthn(symget('pre_qualifier_'||left(K))) - lengthn(symget('joining_phrase_'||left(c))) - 2;
			                                            qualified_term_beg  = position;
			                                            K = 0;
			                                            c = &count_joining;
			                                        end;
			                                        else if substr(&text_field,position - lengthn(symget('pre_qualifier_'||left(K))) - lengthn(symget('joining_phrase_'||left(c))) - 3, 1) = " " then do;
			                                            pre_qualifier       = catx(" ", symget('pre_qualifier_'||left(K)), symget('joining_phrase_'||left(c)), pre_qualifier);
			                                            position            = position - lengthn(symget('pre_qualifier_'||left(K))) - lengthn(symget('joining_phrase_'||left(c))) - 2;
			                                            qualified_term_beg  = position;
			                                            K = 0;
			                                            c = &count_joining;
			                                        end;
			                                    end;
			                                end;
			                            end;
			                            if K ~= 0 then do;
			                                if lengthn(symget('pre_qualifier_'||left(K))) < (position - 1) then do;
			                                    if substr(  &text_field,
			                                                position - lengthn(symget('pre_qualifier_'||left(K))) - 1,
			                                                lengthn(symget('pre_qualifier_'||left(K)))
			                                                ) = symget('pre_qualifier_'||left(K)) then do;
			                                        if position - lengthn(symget('pre_qualifier_'||left(K))) - 2 = 0 then do;
			                                            pre_qualifier       = catx(" ", symget('pre_qualifier_'||left(K)), pre_qualifier);
			                                            position            = position - lengthn(symget('pre_qualifier_'||left(K))) - 1;
			                                            qualified_term_beg  = position;
			                                            K = 0;
			                                            c = &count_joining;
			                                        end;
			                                        else if substr(&text_field,position - lengthn(symget('pre_qualifier_'||left(K))) - 2, 1) = " " then do;
			                                            pre_qualifier       = catx(" ", symget('pre_qualifier_'||left(K)), pre_qualifier);
			                                            position            = position - lengthn(symget('pre_qualifier_'||left(K))) - 1;
			                                            qualified_term_beg  = position;
			                                            K = 0;
			                                            c = &count_joining;
			                                        end;
			                                    end;
			                                end;
			                            end;
			                        end;
			                    end;
			                    else if lengthn(symget('pre_qualifier_'||left(K))) < (position - 1) then do;
			                        if substr(  &text_field,
			                                    position - lengthn(symget('pre_qualifier_'||left(K))) - 1,
			                                    lengthn(symget('pre_qualifier_'||left(K)))
			                                    ) = symget('pre_qualifier_'||left(K)) then do;
			                            if position - lengthn(symget('pre_qualifier_'||left(K))) - 2 = 0 then do;
			                                pre_qualifier       = catx(" ", symget('pre_qualifier_'||left(K)), pre_qualifier);
			                                position            = position - lengthn(symget('pre_qualifier_'||left(K))) - 1;
			                                qualified_term_beg  = position;
			                                K = 0;
			                            end;
			                            else if substr(&text_field,position - lengthn(symget('pre_qualifier_'||left(K))) - 2, 1) = " " then do;
			                                pre_qualifier       = catx(" ", symget('pre_qualifier_'||left(K)), pre_qualifier);
			                                position            = position - lengthn(symget('pre_qualifier_'||left(K))) - 1;
			                                qualified_term_beg  = position;
			                                K = 0;
			                            end;
			                        end;
			                    end;
			                end;
			            end;
			            position = term_position_end;
			            do k = 1 to &count_qualifiers;
			                if symget('post_qualifier_'||left(K)) ~= "" then do;
			                    if term_position_end ~= position then do c = 1 to &count_joining;
			                        if symget('joining_phrase_'||left(c)) ~= "" then do;
			                            if K ~= 0 then do;
			                                if lengthn(symget('joining_phrase_'||left(c))) + 1 + lengthn(symget('post_qualifier_'||left(K))) < (lengthn(&text_field) - position) then do;
			                                    if substr(  &text_field,
			                                                position + 2,
			                                                lengthn(symget('joining_phrase_'||left(c))) + 1 + lengthn(symget('post_qualifier_'||left(K)))
			                                                ) = symget('post_qualifier_'||left(K)) then do;
			                                        if position + 2 + lengthn(symget('joining_phrase_'||left(c))) + lengthn(symget('post_qualifier_'||left(K))) > lengthn(&text_field) then do;
			                                            post_qualifier      = catx(" ", post_qualifier, symget('joining_phrase_'||left(c)), symget('post_qualifier_'||left(K)));
			                                            position            = position + 2 + lengthn(symget('joining_phrase_'||left(c))) + lengthn(symget('post_qualifier_'||left(K)));
			                                            qualified_term_end  = position;
			                                            K = 0;
			                                            c = &count_joining;
			                                        end;
			                                        else 
			                                            if substr(&text_field,position + 2 + lengthn(symget('joining_phrase_'||left(c))) + lengthn(symget('post_qualifier_'||left(K))), 1) = " " then do;
			                                            post_qualifier      = catx(" ", post_qualifier, symget('joining_phrase_'||left(c)), symget('post_qualifier_'||left(K)));
			                                            position            = position + 2 + lengthn(symget('joining_phrase_'||left(c))) + lengthn(symget('post_qualifier_'||left(K)));
			                                            qualified_term_end  = position;
			                                            K = 0;
			                                            c = &count_joining;
			                                        end;
			                                    end;
			                                end;
			                            end;
			                            if K ~= 0 then do;
			                                if K ~= &count_qualifiers then do;
			                                    if lengthn(symget('post_qualifier_'||left(K))) < (lengthn(&text_field) - position) then do;
			                                        if substr(  &text_field,
			                                                    position + 2,
			                                                    lengthn(symget('post_qualifier_'||left(K)))
			                                                    ) = symget('post_qualifier_'||left(K)) then do;
			                                            if position + 2 + lengthn(symget('post_qualifier_'||left(K))) > lengthn(&text_field) then do;
			                                                post_qualifier      = catx(" ", post_qualifier, symget('post_qualifier_'||left(K)));
			                                                position            = position + 1 + lengthn(symget('post_qualifier_'||left(K)));
			                                                qualified_term_end  = position;
			                                                K = 0;
			                                                c = &count_joining;
			                                            end;
			                                            else 
			                                                if substr(&text_field,position + 2 + lengthn(symget('post_qualifier_'||left(K))), 1) = " " then do;
			                                                post_qualifier      = catx(" ", post_qualifier, symget('post_qualifier_'||left(K)));
			                                                position            = position + 1 + lengthn(symget('post_qualifier_'||left(K)));
			                                                qualified_term_end  = position;
			                                                K = 0;
			                                                c = &count_joining;
			                                            end;
			                                        end;
			                                    end;
			                                end;
			                            end;
			                        end;
			                    end;
			                    else if lengthn(symget('post_qualifier_'||left(K))) < (lengthn(&text_field) - position) then do;
			                        if substr(  &text_field,
			                                    position + 2,
			                                    lengthn(symget('post_qualifier_'||left(K)))
			                                    ) = symget('post_qualifier_'||left(K)) then do;
			                            if position + 2 + lengthn(symget('post_qualifier_'||left(K))) > lengthn(&text_field) then do;
			                                post_qualifier      = catx(" ", post_qualifier, symget('post_qualifier_'||left(K)));
			                                position            = position + 1 + lengthn(symget('post_qualifier_'||left(K)));
			                                qualified_term_end  = position;
			                                K = 0;
			                            end;
			                            else 
			                                if substr(&text_field,position + 2 + lengthn(symget('post_qualifier_'||left(K))), 1) = " " then do;
			                                post_qualifier      = catx(" ", post_qualifier, symget('post_qualifier_'||left(K)));
			                                position            = position + 1 + lengthn(symget('post_qualifier_'||left(K)));
			                                qualified_term_end  = position;
			                                K = 0;
			                            end;
			                        end;
			                    end;
			                end;
			            end;
			        end;
			    %end;

			    if qualified_term       = "" then qualified_term = search_term;
			    if qualified_term_beg   = . then qualified_term_beg = term_position_beg;
			    if qualified_term_end   = . then qualified_term_end = term_position_end;
			    qualified_term          = catx(" ",pre_qualifier,search_term,post_qualifier);
			run;

		    %put date MAPPING_QUALIFIERS_MINION macro ended %sysfunc(date(),worddate.);
		    %put time MAPPING_QUALIFIERS_MINION macro ended %sysfunc(time(),time5.0);

		%mend;

		%MAPPING_QUALIFIERS_MINION();

	endrsubmit;
signoff csp&sysparm;
