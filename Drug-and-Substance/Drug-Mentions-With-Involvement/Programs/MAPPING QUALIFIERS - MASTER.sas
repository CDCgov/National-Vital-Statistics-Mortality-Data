
/***************************************************************************************************************
PROGRAM NAME:       MAPPING QUALIFIERS - MASTER
VERSION:			CSP Update 2016 April 01
****************************************************************************************************************/

%macro MAPPING_QUALIFIERS_MASTER( 	BATCHSIZE=, 
                                    MAX_WINDOWS=,
                                    BATCH_REDUX=
                                  );

	/***********************************************************************************************************
	                                                SECTION 1
	************************************************************************************************************/

	OPTIONS AUTOSIGNON;

    %put date MAPPING_QUALIFIERS_MASTER macro started %sysfunc(date(),worddate.);
    %put time MAPPING_QUALIFIERS_MASTER macro started %sysfunc(time(),time5.0);

	%let year_start				= %sysfunc(compress(&year_start))		;
	%let year_end				= %sysfunc(compress(&year_end))		 	;
	%let programpath			= %sysfunc(compress(%str(&programpath)));
	%let projectpath			= %sysfunc(compress(%str(&projectpath)));
	%let LOG_FOLDER				= %sysfunc(compress(%str(&LOG_FOLDER)))	;
	%let autoexec				= %sysfunc(compress(%str(&autoexec_folder)))	;

	%if &BATCHSIZE 					~= %then %let BATCHSIZE					=%sysfunc(compress(&BATCHSIZE))					;
	%if &MAX_WINDOWS 				~= %then %let MAX_WINDOWS				=%sysfunc(compress(&MAX_WINDOWS))				;
	%if &BATCH_REDUX 				~= %then %let BATCH_REDUX				=%sysfunc(compress(&BATCH_REDUX))				;

	/***********************************************************************************************************
	                                                SECTION 2
	************************************************************************************************************/


	/*BATCHSIZE -> batchsize_year*/
    %do year = &year_start %to &year_end;
        %let firstobs_&year = 1;

        %let check_file = %sysfunc(open(data.mentions_&year));

        %if &check_file = 1 %then %do;
            %let obs_&year  = %sysfunc(attrn(&check_file,nlobs));
            %let check_file = %sysfunc(close(1)); 
        %end;

        %else %do;
            %put WARNING: data.mentions_&year does not exist;
            %put NOTE: Failure to locate input dataset;
        %end;
    %end;

    %do year = &year_start %to &year_end;
        %if &BATCHSIZE = %then %do;
            %let batchsize_&year = %sysevalf(&&obs_&year - &&firstobs_&year + 1);
            %put NOTE: Number of observations per batch will be maximized;
        %end;

        %else %if &BATCHSIZE > %sysevalf((&&obs_&year - &&firstobs_&year + 1)) %then %do;
            %let batchsize_&year = %sysevalf(&&obs_&year - &&firstobs_&year + 1);
            %put NOTE: Number of observations per batch will be maximized;
        %end;

        %else %do;
            %let batchsize_&year = &BATCHSIZE;
			%put NOTE: Batchsize for year &year is &&batchsize_&year;
        %end;
    %end;

	/*MAX_Windows*/
	%if &MAX_WINDOWS = %then %do;
		%let MAX_WINDOWS 			= 1;
		%put NOTE: Maximum number of SAS windows to open for batch processing is &MAX_WINDOWS;
	%end;
	
	%else %do;
		%let MAX_WINDOWS 			= &MAX_WINDOWS;
		%put NOTE: Maximum number of SAS windows to open for batch processing is &MAX_WINDOWS;
	%end;

	/*LOG_FOLDER -> LOG_SCRIPT*/
	%let LOG_FOLDER						= %str(&LOG_FOLDER);
    %let LOG_SCRIPT						= %str(-log '&LOG_FOLDER.MQ_log_&j..txt');
    %put NOTE: LOG_SCRIPT				= &LOG_SCRIPT;

	/*BATCH_REDUX*/
    %if &BATCH_REDUX = %then %do;
        %let BATCH_REDUX = ;
        %put NOTE: All batches will be processed.;
    %end;
    %else %do;
        %let BATCH_REDUX = &BATCH_REDUX;
    %end;

	/*Qualifiers: count_qualifiers, length_pre, length_post*/
	%let DSID = %sysfunc(open(input.qualifiers, IS));
	%if &DSID ~= 0 %then %do;
	    %let anobs = %sysfunc(attrn(&DSID, ANOBS));
	    %let whstmt = %sysfunc(attrn(&DSID, WHSTMT));
	    %if &anobs = 1 & &whstmt = 0 %then %let count_qualifiers = %sysfunc(attrn(&DSID, NLOBS));
	    %let DSID = %sysfunc(close(1));
	%end;
	%else %do;
	    %let count_qualifiers = 0;
	    %let length_pre_qualifier = 1;
	    %let length_post_qualifier = 1;
	%end;

	%if &count_qualifiers>0 %then %do;
	    proc sql noprint;
	        create table input.qualifiers (compress = yes) as
	        select distinct *, 
	            lengthn(pre_qualifier)  as length_pre, 
	            lengthn(post_qualifier) as length_post
	            from input.qualifiers
				where pre_qualifier ~= "" or post_qualifier ~= ""
	            order by length_pre desc, length_post desc;
	        select strip(put(count(length_pre),best32.)) into :count_qualifiers
	            from input.qualifiers
				where pre_qualifier ~= "" or post_qualifier ~= "";
	    quit;
	%end;

	/***********************************************************************************************************
	                                                SECTION 3
	************************************************************************************************************/

	options dlcreatedir;
	libname AUTOEXEC "&autoexec_folder";

	%do year = &year_start %to &year_end;
		%if &&obs_&year ~= %then %do;
			%if &year = &year_start %then %do;
				%let b_counter = 0;
				%let batch_start_&year = 0;
			%end;
			%else %let batch_start_&year = &b_counter;
			%let batch_start_&year = %sysevalf(&b_counter + 1);
			%do part = 1 %to %sysfunc(ceil(%sysevalf((&&obs_&year - &&firstobs_&year + 1)/&&BATCHSIZE_&year))); 
				%let b_counter = %sysevalf(&b_counter + 1);                                         
				filename autoexec "&autoexec_folder.MQ_&b_counter..sas";

				data _null_;
					file autoexec;
					put "%nrstr(%let) year                      = &year;";
					put "%nrstr(%let) part                      = &part;";
					put "%nrstr(%let) firstobs                  = %sysevalf(&&firstobs_&year + (&part - 1)*&&BATCHSIZE_&year);";
					put "%nrstr(%let) obs                       = %sysevalf(&part * &&BATCHSIZE_&year);";
					put "%nrstr(%let) count_qualifiers          = &count_qualifiers;";
					put "%nrstr(%let) log_folder     			= &log_folder;";
					put "%nrstr(%let) projectpath    			= &projectpath;";
				run;
			%end;
			%let batch_end_&year = &b_counter;
		%end;
	%end;

	/***********************************************************************************************************
	                                                SECTION 4
	************************************************************************************************************/

	%do year = &year_start %to &year_end;
		%if &&obs_&year ~= %then %do;
			%do i = &&batch_start_&year %to &&batch_end_&year;
				%if &batch_redux~= %then %do;
					%let year = &year_end;
					%let i = &&batch_end_&year;
					%let j = &batch_redux;
				%end;
				%else %let j = &i;

systask command
	"sas 	-sysin '&ProgramPath\MAPPING QUALIFIERS - MINION.sas' 
			-noicon 
			-nosplash 
			-autoexec '&autoexec_folder.MQ_&j..sas'
			&LOG_SCRIPT
			-sysparm &j"
	nowait
	taskname=task&j
;

				%if %sysfunc(round(%sysevalf(&j/&MAX_WINDOWS),1)) = %sysevalf(&j/&MAX_WINDOWS) %then %do;
					waitfor _ALL_                                                                           
					%do k = %sysevalf(&j-(&MAX_WINDOWS-1)) %to &j;
						task&k
					%end;
				%end;
				;
			%end;

		waitfor _ALL_                                                                           
			%if &batch_redux= %then %do k = &&batch_start_&year %to &&batch_end_&year;
				task&k
			%end;
			%else %if &batch_redux~= %then %do;
			%let k = &batch_redux;
				task&k
			%end;
		;
		%end;
	%end;   

	systask kill _all_;

	/***********************************************************************************************************
	                                                SECTION 5
	************************************************************************************************************/

	%if &batch_redux= %then %do;
		data _null_;
			file "&LOG_FOLDER.MQ_MINION_LOG.txt";
			put "Start of MQ MINION LOG";
			;
		run;
	%end;

	%do year = &year_start %to &year_end;
		%if &&obs_&year ~= %then %do;
			%do i = &&batch_start_&year %to &&batch_end_&year;
				%if &batch_redux~= %then %do;
					%let year = &year_end;
					%let i = &&batch_end_&year;
					%let j = &batch_redux;
				%end;
				%else %let j = &i;

				data _null_;
					infile "&LOG_FOLDER.MQ_LOG_&j..txt";
					file "&LOG_FOLDER.MQ_MINION_LOG.txt"
						mod
					;
					input;
					put _infile_;
				run;

				data _null_;
					infile "&LOG_FOLDER.MQ_MINI_&j..txt";
					file "&LOG_FOLDER.MQ_MINION_LOG.txt"
						mod
					;
					input;
					put _infile_;
				run;
				%let rc     = %sysfunc(filename(myRef,&LOG_FOLDER.MQ_LOG_&j..txt));
				%let sysrc  = %sysfunc(fdelete(&myRef));
				%let rc     = %sysfunc(filename(myRef,&LOG_FOLDER.MQ_MINI_&j..txt));
				%let sysrc  = %sysfunc(fdelete(&myRef));
			%end;
		%end;
	%end;

	/***********************************************************************************************************
	                                                SECTION 6
	************************************************************************************************************/

	%put date MAPPING_QUALIFIERS_MASTER_SIGN_ON START %sysfunc(date(),worddate.);
	%put time MAPPING_QUALIFIERS_MASTER_SIGN_ON START %sysfunc(time(),time8.2);

	%include "\\cdc\csp_Project\CIPSEA_PII_DVS_MSB\Drug_Involved_Mortality\Final_Programs\2019-01-18\log_csp_signon.sas" /NOSOURCE2 ;
	%initialize_signon();

	OPTIONS AUTOSIGNON;
	%LET RCMQ=%SYSFUNC(grdsvc_enable(_ALL_,resource=CSP_MNL )) ;
   	%PUT Return Code = &RCMQ;
	signon cspMQ wait=yes MACVAR=SIGNON_RC ;
		%log_signon(session_name=cspMQ);

		%SYSLPUT year            		= &year			  	  	/remote=cspMQ;
		%SYSLPUT batch_redux     		= &batch_redux        	/remote=cspMQ;
		%SYSLPUT year_start      		= &year_start         	/remote=cspMQ;
		%SYSLPUT year_end        		= &year_end           	/remote=cspMQ;
		%SYSLPUT LOG_FOLDER  			= &LOG_FOLDER  			/remote=cspMQ;
		%SYSLPUT MAX_WINDOWS       		= &MAX_WINDOWS        	/remote=cspMQ;
		%do year = &year_start %to &year_end;
			%SYSLPUT obs_&year			= &&obs_&year        	/remote=cspMQ;
			%SYSLPUT firstobs_&year  	= &&firstobs_&year   	/remote=cspMQ;
			%SYSLPUT batchsize_&year	= &&batchsize_&year  	/remote=cspMQ;
			%SYSLPUT batch_start_&year  = &&batch_start_&year   /remote=cspMQ;
			%SYSLPUT batch_end_&year    = &&batch_end_&year     /remote=cspMQ;
		%end;

		rsubmit cspMQ wait=yes inheritlib=(data input raw results temp);
			proc Printto NEW LOG="&LOG_FOLDER.MQ_MASTER.txt";

	%put date MAPPING_QUALIFIERS_MASTER_SIGN_ON DONE  %sysfunc(date(),worddate.);
	%put time MAPPING_QUALIFIERS_MASTER_SIGN_ON DONE %sysfunc(time(),time8.2);

			%macro append();
				%nrstr(%%)if &batch_redux = %nrstr(%%)then %nrstr(%%)do;
					%nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
						%nrstr(%%)if %sysfunc(exist(data.qualified_mentions_&year)) %nrstr(%%)then %nrstr(%%)do;
							proc datasets 
								library = data
								nolist;
								delete qualified_mentions_&year;
							quit;
						%nrstr(%%)end;

						%nrstr(%%)do part = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf((&&obs_&year - &&firstobs_&year + 1)/&&batchsize_&year)));
							proc append
								base = data.qualified_mentions_&year
								data = temp.qualified_mentions_&year._&part
								force;
							quit;

							%nrstr(%%)if &part = 1 %nrstr(%%)then %nrstr(%%)do;
								data data.qualified_mentions_&year (compress = yes);
									set data.qualified_mentions_&year;
								run;
							%nrstr(%%)end;
						%nrstr(%%)end;

						proc sql noprint;
							create table data.qualified_mentions_&year (compress = yes) as
				            select uniq_ID,
				                   search_term,
				                   qualified_term,
				                   pre_qualifier,
				                   post_qualifier,
				                   text_field,
				                   chain,
				                   cleaned_chain,
				                   descr_lin5,
				                   cleaned_descr_lin5,
				                   inj_descr,
				                   cleaned_inj_descr,
				                   term_position_beg,
				                   term_position_end,
				                   qualified_term_beg,
				                   qualified_term_end
								from data.qualified_mentions_&year
						        order by    uniq_ID,
						                    text_field, 
						                    qualified_term_beg, 
						                    qualified_term_end desc;
						quit;
					%nrstr(%%)end;

					%nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
						proc datasets 
							library = temp
							nolist;
							delete
							%nrstr(%%)do part = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf((&&obs_&year - &&firstobs_&year + 1)/&&batchsize_&year)));
								qualified_mentions_&year._&part
							%nrstr(%%)end;
						;
						quit;
					%nrstr(%%)end;
				%nrstr(%%)end;

				%nrstr(%%)else %nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
					%nrstr(%%)if &year = &year_start %nrstr(%%)then %nrstr(%%)do; 
						%nrstr(%%)let b_counter = 0;
						%nrstr(%%)let batch_start_&year = 1;
					%nrstr(%%)end;
					%nrstr(%%)else %nrstr(%%)let batch_start_&year = &b_counter;
					%nrstr(%%)do part = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf((&&obs_&year - &&firstobs_&year + 1)/&&batchsize_&year)));
						%nrstr(%%)let b_counter = %sysevalf(&b_counter + 1);
						%nrstr(%%)if &batch_redux = &b_counter %nrstr(%%)then %nrstr(%%)do;
							proc append
								base = data.qualified_mentions_&year
								data = temp.qualified_mentions_&year._&part
								force;
							quit;

							proc sql noprint;
								create table data.qualified_mentions_&year (compress = yes) as
					            select uniq_ID,
					                   search_term,
					                   qualified_term,
					                   pre_qualifier,
					                   post_qualifier,
					                   text_field,
					                   chain,
					                   cleaned_chain,
					                   descr_lin5,
					                   cleaned_descr_lin5,
					                   inj_descr,
					                   cleaned_inj_descr,
					                   term_position_beg,
					                   term_position_end,
					                   qualified_term_beg,
					                   qualified_term_end
									from data.qualified_mentions_&year
						            order by    uniq_ID,
						                        text_field, 
						                        qualified_term_beg, 
						                        qualified_term_end desc;
							quit;

							proc datasets 
								library = temp
								nolist;
								delete
								qualified_mentions_&year._&part
							;
							quit;
						%nrstr(%%)end;
					%nrstr(%%)end;
					%nrstr(%%)let batch_end_&year = &b_counter;
				%nrstr(%%)end;
			%mend append;

			%append();

		endrsubmit;
	signoff cspMQ;

	/***********************************************************************************************************
	                                                SECTION 7

	This section removes mentions of words incorrectly identified as substances, but are actually descriptors.  
	For example, if MEDICINAL is both a search term and a descriptor, then this step will remove mentions of the 
	search term MEDICINAL in records where it is being used as a descriptor, thereby preventing double-counting.
	************************************************************************************************************/

	%do year = &year_start %to &year_end;

		data data.qualified_mentions_&year (compress = yes);
			set data.qualified_mentions_&year;
			by uniq_ID text_field qualified_term_beg descending qualified_term_end;
			retain qualified_term_beg_retain 0 qualified_term_end_retain 0;
			if first.text_field then do;
				qualified_term_beg_retain = qualified_term_beg;
				qualified_term_end_retain = qualified_term_end;
			end;
			else do;
				if 	qualified_term_beg >= qualified_term_beg_retain and 
					qualified_term_end <= qualified_term_end_retain then delete;
				else do;
					qualified_term_beg_retain = qualified_term_beg;
					qualified_term_end_retain = qualified_term_end;
				end;
			end;
			drop qualified_term_beg_retain qualified_term_end_retain;
		run;

	%end;

	/***********************************************************************************************************
	                                                SECTION 8
	************************************************************************************************************/

    %do year = &year_start %to &year_end;
        %let dsid           =   %sysfunc(open(data.mentions_&year));
        %let obs_&year  	=   %sysfunc(attrn(&dsid,nlobs));
        %let rc             =   %sysfunc(close(&dsid));
    %end;

    %do year = &year_start %to &year_end;
        %let dsid           =   %sysfunc(open(data.qualified_mentions_&year));
        %let new_obs_&year  =   %sysfunc(attrn(&dsid,nlobs));
        %let rc             =   %sysfunc(close(&dsid));
    %end;

    data check_data;
        length log_text $256.;
        %do year = &year_start %to &year_end;
            log_text = "observations in data.mentions_&year    = &&obs_&year";
            output;
            log_text = "observations in data.qualified_mentions_&year = &&new_obs_&year";
            output;
        %end;
    run;

    data work.check_batch_log;
        infile "&LOG_FOLDER.MQ_MINION_LOG.txt" truncover;
        input log $256.;
        length log_text $256.;
        if substr(log,1,15)="batch number = " then do;
            batch = substr(log,16,10);
            retain batch;
        end;
        if substr(log,1,29)="year                       = " then do;
            year = substr(log,30,4);
            retain year;
        end;
        if substr(log,1,29)="part                       = " then do;
            part = substr(log,30,10);
            retain part;
            log_text = catt("batch = ",cat(" ",batch),"     year = ",cat(" ",year),"     part = ",cat(" ",part));
            output;
        end;
        if find(log,"ERROR") then do;
            log_text = log;
            output;
        end;
    run; 

    data work.check_all;
        set work.check_data (keep = log_text)
            work.check_batch_log (keep = log_text)
        ;
    run;

    title "check for errors";
        proc print 
            data=work.check_all noobs; 
        run;
    title;

    %put date MAPPING_QUALIFIERS_MASTER macro ended %sysfunc(date(),worddate.);
    %put time MAPPING_QUALIFIERS_MASTER macro ended %sysfunc(time(),time8.2);
%mend;

