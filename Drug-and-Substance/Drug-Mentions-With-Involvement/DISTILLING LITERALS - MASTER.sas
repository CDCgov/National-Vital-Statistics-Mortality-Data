/***************************************************************************************************************
PROGRAM NAME:       DISTILLING LITERALS - MASTER
VERSION:			CSP Update 2016 April 01
****************************************************************************************************************/

%macro DISTILLING_LITERALS(CSP_OPTIONS=CSP_MOD);
	/***********************************************************************************************************
	                                                SECTION 1
	************************************************************************************************************/

	OPTIONS AUTOSIGNON;

    %put date DISTILLING_LITERALS macro started %sysfunc(date(),worddate.);
    %put time DISTILLING_LITERALS macro started %sysfunc(time(),time5.0);

	%let year_start				= %sysfunc(compress(&year_start))		;
	%let year_end				= %sysfunc(compress(&year_end))		 	;
	%let programpath			= %sysfunc(compress(%str(&programpath)));
	%let projectpath			= %sysfunc(compress(%str(&projectpath)));
	%let LOG_FOLDER				= %sysfunc(compress(%str(&LOG_FOLDER)))	;
	%let autoexec				= %sysfunc(compress(%str(&autoexec_folder)))	;

	/***********************************************************************************************************
	                                                SECTION 2
	************************************************************************************************************/

	%put date DISTILLING_LITERALS_SIGN_ON START %sysfunc(date(),worddate.);
    %put time DISTILLING_LITERALS_SIGN_ON START %sysfunc(time(),time8.2);

	%include "\\cdc\csp_Project\CIPSEA_PII_DVS_MSB\Drug_Involved_Mortality\Final_Programs\2019-01-18\log_csp_signon.sas" /NOSOURCE2 ;
	%initialize_signon();

	OPTIONS AUTOSIGNON;
	%LET RCDL=%SYSFUNC(grdsvc_enable(_ALL_,resource=CSP_MNL )) ;
   	%PUT Return Code = &RCDL;
	signon cspDL wait=yes MACVAR=SIGNON_RC ;
		%log_signon(session_name=cspDL);

        %SYSLPUT year_start      	= &year_start         	/remote=cspDL;
        %SYSLPUT year_end        	= &year_end           	/remote=cspDL;
        %SYSLPUT LOG_FOLDER        	= &LOG_FOLDER          	/remote=cspDL;

        rsubmit cspDL wait=yes inheritlib=(data input raw results temp);
			proc Printto NEW LOG="&LOG_FOLDER.DL_BODY.txt";

	%put date DISTILLING_LITERALS_SIGN_ON DONE %sysfunc(date(),worddate.);
	%put time DISTILLING_LITERALS_SIGN_ON DONE %sysfunc(time(),time8.2);

			%macro distillLiterals();
			    %do year = &year_start %to &year_end;
			        data temp.qualified_mentions_&year (compress = yes);
			            set data.qualified_mentions_&year;
			            length literal $480.;
			            %do i = 1 %to 3;
			                %if &i = 1 %then %let check = CLEANED_CHAIN;
			                %if &i = 2 %then %let check = CLEANED_DESCR_LIN5;
			                %if &i = 3 %then %let check = CLEANED_INJ_DESCR;
			                if text_field = "&check" then do;
			                    literal = &check;
			                end;
			            %end;
			        run;

			        proc sql noprint;
			            select max(lengthn(literal))        into :max_length_literal        from temp.qualified_mentions_&year;
			            select max(lengthn(qualified_term)) into :max_length_qualified_term from temp.qualified_mentions_&year;
			        quit;

			        proc sort data=temp.qualified_mentions_&year;
			        by uniq_id text_field qualified_term_beg descending qualified_term_end;
			        run;

			        data    temp.position_shift_&year       (compress = yes keep=uniq_id text_field qualified_term_beg position_shift)
			                temp.distilled_literals_&year   (compress = yes keep=uniq_id text_field distilled_literal);
			            set temp.qualified_mentions_&year;
			            by uniq_id text_field qualified_term_beg descending qualified_term_end;
			            length distilled_literal $&max_length_literal..;
			            retain distilled_literal;
			            retain retained_position_shift;
			            retain position_shift;
			            retain retained_qualified_term_end;
			            if first.text_field then do;
			                distilled_literal = literal;
			                position_shift = 0;
			                substr(distilled_literal, qualified_term_beg, qualified_term_end - qualified_term_beg + 1) = "*";
			                retained_position_shift = qualified_term_end - qualified_term_beg;
			                retained_qualified_term_end = qualified_term_end;
			                output temp.position_shift_&year;
			            end;
			            else do;
			                if retained_qualified_term_end > qualified_term_beg then do;
			                    position_shift = retained_position_shift - (retained_qualified_term_end - qualified_term_beg + 2);
								retained_position_shift = retained_position_shift + (qualified_term_end - (retained_qualified_term_end + 2));
								retained_qualified_term_end = qualified_term_end;
			                end;
			                else do;
								position_shift = retained_position_shift;
								retained_position_shift = position_shift + qualified_term_end - qualified_term_beg;
								retained_qualified_term_end = qualified_term_end;
							end;
			                substr(distilled_literal, qualified_term_beg, qualified_term_end - qualified_term_beg + 1)="*";
			                output temp.position_shift_&year;
			            end;
			            if last.text_field then do;
			                do i=1 to &max_length_qualified_term;
			                    distilled_literal=tranwrd(distilled_literal,"  "," ");
			                end;
			                output temp.distilled_literals_&year;
			            end;
			        run;

			        proc sql;
			            create table data.distilled_literals_&year (compress = yes) as
			            select * 
			                from    data.qualified_mentions_&year as a
			                        left join
			                        temp.distilled_literals_&year as b
			                        on  a.uniq_id = b.uniq_id and 
			                            a.text_field = b.text_field 
			                        left join
			                        temp.position_shift_&year as c
			                        on  a.uniq_id = c.uniq_id and 
			                            a.text_field = c.text_field and 
			                            a.qualified_term_beg = c.qualified_term_beg
			                order by uniq_id, a.text_field, a.qualified_term_beg, qualified_term_end desc;
			            drop table temp.distilled_literals_&year;
			            drop table temp.qualified_mentions_&year;
			            drop table temp.position_shift_&year;
			        quit;
			    %end;

	/***********************************************************************************************************
	                                                SECTION 2
	************************************************************************************************************/

			    %do year = &year_start %to &year_end;
			        %let DSID = %sysfunc(open(input.joining_phrases, IS));
			        %if &DSID ~= 0 %then %do;
			            %let anobs = %sysfunc(attrn(&DSID, ANOBS));
			            %let whstmt = %sysfunc(attrn(&DSID, WHSTMT));
			            %if &anobs = 1 & &whstmt = 0 %then %let counted = %sysfunc(attrn(&DSID, NLOBS));
			            %let DSID = %sysfunc(close(1));
			            %if &counted>0 %then %do;
			                proc sql noprint;
			                    select  strip(upcase(joining_phrase)) 
			                            into :joining_phrase_1 - :joining_phrase_&counted
			                            from input.joining_phrases;
			                    select  lengthn(strip(joining_phrase))
			                            into
			                            :joining_length_1 - :joining_length_&counted
			                        from input.joining_phrases;
			                quit;
			            %end;
			        %end;
			        %else %do;
			            %let counted = 0;
			        %end;

			        data data.distilled_literals_&year (compress = yes);
			            set data.distilled_literals_&year;
			                sum_position_shift = position_shift;
			                do position = 1 to lengthn(distilled_literal) while (position < (qualified_term_beg - position_shift));
			                    if substr(distilled_literal,position,1) = "*" then do;
			                        %if &counted > 0 %then %do i = 1 %to &counted;
			                            if substr(distilled_literal, position, &&joining_length_&i) = "&&joining_phrase_&i" 
			                            then sum_position_shift = sum_position_shift + %sysevalf(&&joining_length_&i - 1);
			                        %end;
			                        %else %do;
			                            sum_position_shift = 0;
			                        %end;
			                    end;
			                end;
			            distilled_position = qualified_term_beg - sum_position_shift;
			            %if &counted > 0 %then %do i = 1 %to &counted;
			                do i = 1 to (count(distilled_literal,"&&joining_phrase_&i") +1);
			                    distilled_literal = tranwrd(distilled_literal,"&&joining_phrase_&i","*");
			                end;
			            %end;
			            drop    i
			                    position_shift 
			                    sum_position_shift 
			                    position 
			                    ;
			        run;

			        proc sql;
			            create table data.distilled_literals_&year (compress = yes) as
			            select distinct uniq_ID,
			                   search_term,
			                   qualified_term,
			                   pre_qualifier,
			                   post_qualifier,
			                   text_field,
			                   distilled_literal,
			                   chain,
			                   cleaned_chain,
			                   descr_lin5,
			                   cleaned_descr_lin5,
			                   inj_descr,
			                   cleaned_inj_descr,
			                   term_position_beg,
			                   term_position_end,
			                   qualified_term_beg,
			                   qualified_term_end,
			                   distilled_position

/*							   ,*/
/*							   position_shift,*/
/*							   sum_position_shift,*/
/*							   position*/

			            from data.distilled_literals_&year
			            order by    uniq_ID,
			                        text_field, 
			                        qualified_term_beg, 
			                        qualified_term_end desc;
			        quit;
			    %end;
			%mend distillLiterals;

			%distillLiterals();

		endrsubmit;
 	signoff cspDL;

    %put date DISTILLING_LITERALS macro ended %sysfunc(date(),worddate.);
    %put time DISTILLING_LITERALS macro ended %sysfunc(time(),time5.0);
%mend;
