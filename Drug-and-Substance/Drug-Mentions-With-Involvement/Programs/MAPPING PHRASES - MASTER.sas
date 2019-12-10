/***************************************************************************************************************
PROGRAM NAME:       MAPPING PHRASES - MASTER
VERSION:			CSP Update 2016 April 01
****************************************************************************************************************/

%macro MAPPING_PHRASES_MASTER( INPUT_DATASET_FOLDER=,
                               INPUT_DATASET_FILE=,
                               OUTPUT_DATASET_FOLDER=,
                               INPUT_PHRASE_LIST=,
							   UNMAPPED_RECORDS_ONLY=,
                               BATCHSIZE=, 
                               MAX_WINDOWS=,
                               PHRASE_BATCHSIZE=, 
                               BATCH_REDUX=
                             );

	/***********************************************************************************************************
	                                                SECTION 1
	************************************************************************************************************/

	OPTIONS AUTOSIGNON;

    %put date MAPPING_PHRASES macro started %sysfunc(date(),worddate.);
    %put time MAPPING_PHRASES macro started %sysfunc(time(),time5.0);

	%let year_start				= %sysfunc(compress(&year_start))		;
	%let year_end				= %sysfunc(compress(&year_end))		 	;
	%let programpath			= %sysfunc(compress(%str(&programpath)));
	%let projectpath			= %sysfunc(compress(%str(&projectpath)));
	%let LOG_FOLDER				= %sysfunc(compress(%str(&LOG_FOLDER)))	;
	%let autoexec				= %sysfunc(compress(%str(&autoexec_folder)))	;

	%if &INPUT_DATASET_FOLDER ~= 	%then %let INPUT_DATASET_FOLDER 	=%sysfunc(compress(&INPUT_DATASET_FOLDER))	;
	%if &INPUT_DATASET_FILE ~= 		%then %let INPUT_DATASET_FILE		=%sysfunc(compress(&INPUT_DATASET_FILE))	;
	%if &OUTPUT_DATASET_FOLDER ~= 	%then %let OUTPUT_DATASET_FOLDER	=%sysfunc(compress(&OUTPUT_DATASET_FOLDER))	;
	%if &INPUT_PHRASE_LIST ~= 		%then %let INPUT_PHRASE_LIST		=%sysfunc(compress(&INPUT_PHRASE_LIST))		;
	%if &UNMAPPED_RECORDS_ONLY ~= 	%then %let UNMAPPED_RECORDS_ONLY	=%sysfunc(compress(&UNMAPPED_RECORDS_ONLY))	;
	%if &BATCHSIZE ~= 				%then %let BATCHSIZE				=%sysfunc(compress(&BATCHSIZE))				;
	%if &MAX_WINDOWS ~= 			%then %let MAX_WINDOWS				=%sysfunc(compress(&MAX_WINDOWS))			;
	%if &PHRASE_BATCHSIZE ~= 		%then %let PHRASE_BATCHSIZE			=%sysfunc(compress(&PHRASE_BATCHSIZE))		;
	%if &BATCH_REDUX ~= 			%then %let BATCH_REDUX				=%sysfunc(compress(&BATCH_REDUX))			;

	/***********************************************************************************************************
	                                                SECTION 2
	************************************************************************************************************/

	/*INPUT_DATASET_FOLDER*/
	%let INPUT_DATASET_FOLDER 	= &INPUT_DATASET_FOLDER;
	%put NOTE: Records to be processed will come from &INPUT_DATASET_FOLDER folder.;

	/*INPUT_DATASET_FILE*/
	%let INPUT_DATASET_FILE 	= &INPUT_DATASET_FILE;
    %put NOTE: File containing records to be processed will be the &INPUT_DATASET_FILE file.;

        /************************************************************************************************************
            Macro variable                  |   Output values for macro variable
         -----------------------------------|------------------------------------------------------------------------
        |   firstobs_&year                  |   1                                                                    |
        |   obs_&year                       |   Total number of records in &INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE|
        ************************************************************************************************************/

	/*firstobs_year*/
	/*obs_year*/
	/*Check INPUT_DATASET_FOLDER.INPUT_DATASET_FILE*/
    %do year = &year_start %to &year_end;
        %let firstobs_&year = 1;

        %let check_file = %sysfunc(open(&INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year));

        %if &check_file = 1 %then %do;
            %let obs_&year  = %sysfunc(attrn(&check_file,nlobs));
            %let check_file = %sysfunc(close(1)); 
        %end;

        %else %do;
            %put WARNING: &INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year does not exist;
			%let INPUT_DATASET_FOLDER 	= data;
			%let INPUT_DATASET_FILE 	= distilled_literals;
            %put NOTE: File containing records to be processed will be the %str(data.distilled_literals_&year) file.;

	        %let check_file = %sysfunc(open(&INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year));

	        %if &check_file = 1 %then %do;
	            %let obs_&year  = %sysfunc(attrn(&check_file,nlobs));
	            %let check_file = %sysfunc(close(1)); 
	        %end;
        %end;
	    %put NOTE: For year &year, the first record read from input_dataset data will be record 1.;
	    %put NOTE: For year &year, the last record read from input_dataset data will be record &&obs_&year.;
    %end;

	/*BATCHSIZE -> batchsize_year*/
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

	/*OUTPUT_DATASET_FOLDER*/
    %let OUTPUT_DATASET_FOLDER = &OUTPUT_DATASET_FOLDER;
	%put NOTE: The processed data will be in the &OUTPUT_DATASET_FOLDER folder;

	/*INPUT_PHRASE_LIST*/
    %let INPUT_PHRASE_LIST = &INPUT_PHRASE_LIST;
	%if &INPUT_PHRASE_LIST = %then %put NOTE: All phrases in input.phrases dataset will be used.;
	%else %put NOTE: The phrases data subset by INPUT_PHRASE_LIST &INPUT_PHRASE_LIST;

	/*count_phrase*/
	/*length_phrase*/
    proc sql noprint;
        select count(phrase)
            into :count_phrase
            from input.phrases;
        select max(length_phrase)
            into :length_phrase from
                (select lengthn(phrase) as length_phrase
                    from input.phrases
                );
    quit;

	/*phrase_batchsize*/
    %if &PHRASE_BATCHSIZE = %then %do;
        %let PHRASE_BATCHSIZE = &count_phrase;
        %put NOTE: All search terms will be searched for each batch;
    %end;

    %else %do;
        %let PHRASE_BATCHSIZE = &PHRASE_BATCHSIZE;
        %put NOTE: &PHRASE_BATCHSIZE search terms will be searched for at a time;
    %end;

	/*LOG_FOLDER -> LOG_SCRIPT*/
	%let LOG_FOLDER						= %str(&LOG_FOLDER);
    %let LOG_SCRIPT						= %str(-log '&LOG_FOLDER.MP_log_&j..txt');
    %put NOTE: LOG_SCRIPT				= &LOG_SCRIPT;

	/*BATCH_REDUX*/
    %if &BATCH_REDUX = %then %do;
        %let BATCH_REDUX = ;
        %put NOTE: All batches will be processed.;
    %end;
    %else %do;
        %let BATCH_REDUX = &BATCH_REDUX;
    %end;

	/***********************************************************************************************************
	                                                SECTION 3
	************************************************************************************************************/

    options dlcreatedir;
    libname AUTOEXEC "&autoexec_folder";

    %do year = &year_start %to &year_end;
        %if &year = &year_start %then %do;
            %let b_counter = 0;
            %let batch_start_&year = 0;
        %end;
        %else %let batch_start_&year = &b_counter;
		%let batch_start_&year = %sysevalf(&b_counter + 1);
        %do part = 1 %to %sysfunc(ceil(%sysevalf((&&obs_&year - &&firstobs_&year + 1)/&&BATCHSIZE_&year))); 
            %let b_counter = %sysevalf(&b_counter + 1);                                         
            filename autoexec "&autoexec_folder.MP_&b_counter..sas";

            data _null_;
                file autoexec;
                put "%nrstr(%let) year                      = &year;";
                put "%nrstr(%let) part                      = &part;";
                put "%nrstr(%let) firstobs                  = %sysevalf(&&firstobs_&year + (&part - 1)*&&BATCHSIZE_&year);";
                put "%nrstr(%let) obs                       = %sysevalf(&part * &&BATCHSIZE_&year);";
                put "%nrstr(%let) input_dataset_folder      = &INPUT_DATASET_FOLDER;";
                put "%nrstr(%let) input_dataset_file        = &INPUT_DATASET_FILE;";
                put "%nrstr(%let) input_phrase_list         = &INPUT_PHRASE_LIST;";
                put "%nrstr(%let) unmapped_records_only     = &UNMAPPED_RECORDS_ONLY;";
                put "%nrstr(%let) count_phrase              = &count_phrase;";
                put "%nrstr(%let) phrase_batchsize          = &PHRASE_BATCHSIZE;";
                put "%nrstr(%let) length_phrase             = &length_phrase;";
			    put "%nrstr(%let) log_folder     			= &log_folder;";
			    put "%nrstr(%let) projectpath    			= &projectpath;";
            run;
            quit;
        %end;
        %let batch_end_&year = &b_counter;
    %end;

	/***********************************************************************************************************
	                                                SECTION 4
	************************************************************************************************************/

   %do year = &year_start %to &year_end;
       %do i = &&batch_start_&year %to &&batch_end_&year;
            %if &batch_redux~= %then %do;
                %let year = &year_end;
                %let i = &&batch_end_&year;
                %let j = &batch_redux;
            %end;
            %else %let j = &i;

systask command                                                 
    "sas 	-sysin '&ProgramPath\MAPPING PHRASES - MINION.sas'
			-noicon
			-nosplash
			-autoexec '&autoexec_folder.MP_&j..sas'
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

   systask kill _all_;

	/***********************************************************************************************************
	                                                SECTION 5
	************************************************************************************************************/

	%if &batch_redux= %then %do;
		data _null_;
			file "&LOG_FOLDER.MP_MINION_LOG.txt";
			put "Start of MP MINION LOG";
			;
		run;
	%end;

	%do year = &year_start %to &year_end;
		%do i = &&batch_start_&year %to &&batch_end_&year;
			%if &batch_redux~= %then %do;
				%let year = &year_end;
				%let i = &&batch_end_&year;
				%let j = &batch_redux;
			%end;
			%else %let j = &i;

			data _null_;
				infile "&LOG_FOLDER.MP_LOG_&j..txt";
				file "&LOG_FOLDER.MP_MINION_LOG.txt"
					mod
				;
				input;
				put _infile_;
			run;

			data _null_;
				infile "&LOG_FOLDER.MP_MINI_&j..txt";
				file "&LOG_FOLDER.MP_MINION_LOG.txt"
					mod
				;
				input;
				put _infile_;
			run;
			%let rc     = %sysfunc(filename(myRef,&LOG_FOLDER.MP_LOG_&j..txt));
			%let sysrc  = %sysfunc(fdelete(&myRef));
			%let rc     = %sysfunc(filename(myRef,&LOG_FOLDER.MP_MINI_&j..txt));
			%let sysrc  = %sysfunc(fdelete(&myRef));
		%end;
	%end;

	/***********************************************************************************************************
	                                                SECTION 6
	************************************************************************************************************/

    %put date MAPPING_PHRASES_MASTER_SIGN_ON START %sysfunc(date(),worddate.);
    %put time MAPPING_PHRASES_MASTER_SIGN_ON START %sysfunc(time(),time8.2);

	%include "\\cdc\csp_Project\CIPSEA_PII_DVS_MSB\Drug_Involved_Mortality\Final_Programs\2019-01-18\log_csp_signon.sas" /NOSOURCE2 ;
	%initialize_signon();

    OPTIONS AUTOSIGNON;
	%LET RCMP=%SYSFUNC(grdsvc_enable(_ALL_,resource=CSP_MNL )) ;
   	%PUT Return Code = &RCMP;
    signon cspMP wait=yes MACVAR=SIGNON_RC ;
		%log_signon(session_name=cspMP);

		%SYSLPUT year            		= &year			  			/remote=cspMP;
        %SYSLPUT batch_redux     		= &batch_redux       		/remote=cspMP;
        %SYSLPUT year_start      		= &year_start        		/remote=cspMP;
        %SYSLPUT year_end        		= &year_end          		/remote=cspMP;
        %SYSLPUT b_counter       		= &b_counter         		/remote=cspMP;
		%SYSLPUT OUTPUT_DATASET_FOLDER  = &OUTPUT_DATASET_FOLDER  	/remote=cspMP;
		%SYSLPUT PHRASE_batchsize  		= &PHRASE_batchsize  		/remote=cspMP;
		%SYSLPUT COUNT_PHRASE  			= &COUNT_PHRASE  			/remote=cspMP;
        %SYSLPUT LOG_FOLDER  			= &LOG_FOLDER  				/remote=cspMP;
		%SYSLPUT projectpath    		= &projectpath  			/remote=cspMP;
        %SYSLPUT INPUT_PHRASE_LIST  	= &INPUT_PHRASE_LIST  		/remote=cspMP;
		%do year = &year_start %to &year_end;
	        %SYSLPUT obs_&year       	= &&obs_&year        		/remote=cspMP;
	        %SYSLPUT firstobs_&year  	= &&firstobs_&year   		/remote=cspMP;
	        %SYSLPUT batchsize_&year 	= &&batchsize_&year  		/remote=cspMP;
		%end;

        rsubmit cspMP wait=yes inheritlib=(data input raw results temp);
			proc Printto NEW LOG="&LOG_FOLDER.MP_MASTER.txt";

    %put date MAPPING_PHRASES_MASTER_SIGN_ON DONE %sysfunc(date(),worddate.);
    %put time MAPPING_PHRASES_MASTER_SIGN_ON DONE %sysfunc(time(),time8.2);

			%macro append();
			    %nrstr(%%)if &batch_redux = %nrstr(%%)then %nrstr(%%)do;
			        %nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
			            proc datasets 
			                library = &OUTPUT_DATASET_FOLDER
			                nolist;
				            delete phrase_mentions_&year;
			            quit;

			            %nrstr(%%)let merged_files = 0;

			            %nrstr(%%)do part = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf((&&obs_&year - &&firstobs_&year + 1)/&&BATCHSIZE_&year)));
			                %nrstr(%%)do phrase_batch = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf(&count_phrase / &phrase_batchsize)));
			                    proc append
			                        base = &OUTPUT_DATASET_FOLDER..phrase_mentions_&year
			                        data = temp.phrase_mentions_&year._&part._&phrase_batch
			                        force;
			                    quit;

			                    %nrstr(%%)if &part = 1 and &phrase_batch = 1 %nrstr(%%)then %nrstr(%%)do;
			                        data &OUTPUT_DATASET_FOLDER..phrase_mentions_&year (compress = yes);
			                            set &OUTPUT_DATASET_FOLDER..phrase_mentions_&year;
			                        run;
			                    %nrstr(%%)end;
			                %nrstr(%%)end;
			            %nrstr(%%)end;

			            proc sql noprint;
			                create table &OUTPUT_DATASET_FOLDER..phrase_mentions_&year (compress=yes) as
			                select  *   
			                    from &OUTPUT_DATASET_FOLDER..phrase_mentions_&year
			                    order by uniq_ID, 
			                             text_field, 
			                             search_term, 
			                             phrase_beg, 
			                             phrase_end desc;
			            quit;
			        %nrstr(%%)end;

			        %nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
			            proc datasets 
			                library = temp
			                nolist;
				            delete 
			                %nrstr(%%)do part = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf((&&obs_&year - &&firstobs_&year + 1)/&&BATCHSIZE_&year)));
			                    %nrstr(%%)do phrase_batch = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf(&count_phrase / &phrase_batchsize)));
			                        phrase_mentions_&year._&part._&phrase_batch
			                    %nrstr(%%)end;
			                %nrstr(%%)end;
				            ;
			            quit;
			        %nrstr(%%)end;
			    %nrstr(%%)end;

			    %else %nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
			        %nrstr(%%)if &year = &year_start %nrstr(%%)then %nrstr(%%)do;
			            %nrstr(%%)let b_counter = 0;
			            %nrstr(%%)let batch_start_&year = 1;
			        %nrstr(%%)end;
			        %else %nrstr(%%)let batch_start_&year = &b_counter;
			        %nrstr(%%)do part = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf((&&obs_&year - &&firstobs_&year + 1)/&&BATCHSIZE_&year))); 
			            %nrstr(%%)let b_counter = %sysevalf(&b_counter + 1);
			            %nrstr(%%)if &batch_redux = &b_counter %nrstr(%%)then %nrstr(%%)do;
			                %nrstr(%%)do phrase_batch = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf(&count_phrase / &phrase_batchsize)));
			                    proc append
			                        base = &OUTPUT_DATASET_FOLDER..phrase_mentions_&year
			                        data = temp.phrase_mentions_&year._&part._&phrase_batch
			                        force;
			                    quit;
			                %nrstr(%%)end;

			                proc sql noprint;
			                    create table &OUTPUT_DATASET_FOLDER..phrase_mentions_&year (compress=yes) as
			                    select  *   
			                        from &OUTPUT_DATASET_FOLDER..phrase_mentions_&year
			                        order by uniq_ID, 
			                             text_field, 
			                             search_term, 
			                             phrase_beg, 
			                             phrase_end desc;
			                quit;

			                proc datasets 
			                    library = temp
			                    nolist;
			                delete 
			                    %nrstr(%%)do phrase_batch = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf(&count_phrase / &phrase_batchsize)));
			                        phrase_mentions_&year._&part._&phrase_batch
			                    %nrstr(%%)end;
			                ;
			                quit;
			            %nrstr(%%)end;
			        %nrstr(%%)end;
				    %nrstr(%%)let batch_end_&year = &b_counter;
			    %nrstr(%%)end;
			%mend append;

			%append();

	/***********************************************************************************************************
	                                                SECTION 6
	************************************************************************************************************/

			%macro RESOLVING_PHRASES();
				%nrstr(%%)if &INPUT_PHRASE_LIST = 1 %nrstr(%%)then %nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
					proc sort 
						data = &OUTPUT_DATASET_FOLDER..phrase_mentions_&year;
						by uniq_ID text_field phrase_beg phrase_end distilled_position qualified_term_beg;
					quit;

					data 
						&OUTPUT_DATASET_FOLDER..phrase_mentions_&year (compress = yes);
			            set &OUTPUT_DATASET_FOLDER..phrase_mentions_&year;
						by uniq_ID text_field phrase_beg phrase_end distilled_position qualified_term_beg;
						if first.distilled_position then first_distilled = 1;
						else first_distilled = 0;
						if last.distilled_position then last_distilled = 1;
						else last_distilled = 0;
						if substr(phrase,1,1) = "*" then open_left = 1;
						else open_left = 0;
						if substr(phrase,length(phrase),1) = "*" then open_right = 1;
						else open_right = 0;
						if (first_distilled = 1 and last_distilled = 1) or
							(first_distilled = 1 and last_distilled = 0 and open_right = 1) or
							(first_distilled = 0 and last_distilled = 1 and open_left = 1) or
							(open_right = 1 and open_left = 1) then output &OUTPUT_DATASET_FOLDER..phrase_mentions_&year;
					run; 
				%nrstr(%%)end;

	/***********************************************************************************************************
	                                                SECTION 7
	************************************************************************************************************/

			    %nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
					proc sort 
						data = &OUTPUT_DATASET_FOLDER..phrase_mentions_&year;
						by 	uniq_ID text_field term_position_beg phrase_beg descending phrase_end;
					quit;

			        data &OUTPUT_DATASET_FOLDER..phrase_mentions_&year (compress = yes);
			            set &OUTPUT_DATASET_FOLDER..phrase_mentions_&year;
			            by 	uniq_ID text_field term_position_beg phrase_beg descending phrase_end;

			            retain check_position_beg;
			            retain check_position_end;
			            %nrstr(%%)do i = 1 %nrstr(%%)to 3;
			                %nrstr(%%)if &i = 1 %nrstr(%%)then %nrstr(%%)let text_field = CLEANED_CHAIN;
			                %nrstr(%%)if &i = 2 %nrstr(%%)then %nrstr(%%)let text_field = CLEANED_DESCR_LIN5;
			                %nrstr(%%)if &i = 3 %nrstr(%%)then %nrstr(%%)let text_field = CLEANED_INJ_DESCR;
			                if text_field = "&text_field" then do;
			                    literal_text = %nrstr(%%)substr(&text_field,9,%nrstr(%%)sysevalf(%nrstr(%%)length(&text_field)-8));
			                end;
			            %nrstr(%%)end;
			            if 	first.term_position_beg	then do;
			                check_position_beg = phrase_beg;
			                check_position_end = phrase_end;
			                overlap = 'first term or no overlap';
			            end;
			            else do;
			                if      phrase_beg >= check_position_beg and 
			                        phrase_end <= check_position_end 
			                        then overlap = 'total overlap';
			                else if check_position_beg <= phrase_beg <= check_position_end and 
			                        phrase_end > check_position_end 
			                        then overlap = 'partial overlap';
			                else if phrase_beg > check_position_end then do;
			                    check_position_beg = phrase_beg;
			                    check_position_end = phrase_end;
			                    overlap = 'first term or no overlap';
			                end;
			            end;
			        run;

			        proc sql;
						create table temp.check_phrases_&year as
			            select  distinct a.uniq_ID, 
			                    a.phrase, 
			                    b.overlapping_phrase,
			                    cat(substr(a.phrase, 1, b.phrase_beg - a.phrase_beg),
			                        b.overlapping_phrase) as potential_phrase,
				                    a.literal_text
			                from
			                    (select uniq_ID,
			                            text_field,
			                            literal_text,
			                            phrase,
			                            phrase_beg,
			                            phrase_end
			                        from &OUTPUT_DATASET_FOLDER..phrase_mentions_&year
			                        where overlap = 'first term or no overlap'
			                    ) as a
			                    inner join 
			                    (select uniq_ID,
			                            text_field,
			                            phrase as overlapping_phrase,
			                            phrase_beg,
			                            phrase_end
			                        from &OUTPUT_DATASET_FOLDER..phrase_mentions_&year
			                        where overlap = 'partial overlap'
			                    ) as b
			                    on  a.uniq_ID = b.uniq_ID and 
			                        a.text_field = b.text_field and
			                        a.phrase_beg <= b.phrase_beg <= a.phrase_end and 
			                        a.phrase_end < b.phrase_end
			                order by a.uniq_ID,
			                         a.phrase,
			                         b.overlapping_phrase;
			        quit;
			    %nrstr(%%)end;

				proc sql;
					create table temp.check_phrases as
					select *
						from
							%nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
								%nrstr(%%)if &year ~= &year_start %nrstr(%%)then %nrstr(%%)do;
									union corr
								%nrstr(%%)end;
								(select *
									from temp.check_phrases_&year
								)
							%nrstr(%%)end;
					;
				quit;

			    proc export data = temp.check_phrases
				    outfile = "&ProjectPath\check_phrases.xls"
				    dbms = excelcs label replace;
				    SHEET='Check_Phrases';
			    run;

				%nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
					proc datasets 
						library = temp
						nolist;
						delete check_phrases_&year;
					quit;
				%nrstr(%%)end;

				proc datasets 
					library = temp
					nolist;
					delete check_phrases;
				quit;

	/***********************************************************************************************************
	                                                SECTION 8
	************************************************************************************************************/

			    %nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
			        proc sql noprint;
			            create table &OUTPUT_DATASET_FOLDER..phrase_mentions_&year 
			                (compress = yes
			                drop =  phrase_length
			                        check_position_beg
			                        check_position_end
			                        overlap) as
			            select  uniq_ID,
			                    search_term,
			                    phrase,
								phrase_list,
			                    qualified_term,
			                    pre_qualifier,
			                    post_qualifier,
			                    text_field,
			                    literal_text,
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
			                    distilled_position,
			                    phrase_beg,
			                    phrase_end      
			                from &OUTPUT_DATASET_FOLDER..phrase_mentions_&year
							/*New code: Remove 'partial overlap'*/
							/*Note: Can add macro_variable &allow_partial_overlap to keep partial overlaps*/
			                where overlap in (	'first term or no overlap'
												/*,'partial overlap'*/)
			                order by uniq_id, text_field, term_position_beg;
			        quit;
			    %nrstr(%%)end;
			%mend RESOLVING_PHRASES;

			%RESOLVING_PHRASES;
		endrsubmit;
    signoff cspMP;

	/***********************************************************************************************************
	                                                SECTION 9
	************************************************************************************************************/

    %do year = &year_start %to &year_end;
        %let dsid           				=   %sysfunc(open(data.distilled_literals_&year));
        %let obs_distilled_literals_&year  	=   %sysfunc(attrn(&dsid,nlobs));
        %let rc             				=   %sysfunc(close(&dsid));
    %end;

    %do year = &year_start %to &year_end;
        %let dsid           =   %sysfunc(open(&OUTPUT_DATASET_FOLDER..phrase_mentions_&year));
        %let new_obs_&year  =   %sysfunc(attrn(&dsid,nlobs));
        %let rc             =   %sysfunc(close(&dsid));
    %end;

    %do year = &year_start %to &year_end;
        proc sql noprint;
            select  count(search_term)
                    into :count_no_phrase_mapped_&year
                from &OUTPUT_DATASET_FOLDER..phrase_mentions_&year
                where phrase = "*";
        quit;
    %end;

    data check_data;
        length log_text $256.;
        %do year = &year_start %to &year_end;
            %if &INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year = data.phrase_mentions_&year or
                &INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year = temp.phrase_mentions_&year %then %do;
                log_text = "observations in data.distilled_literals_&year = &&obs_distilled_literals_&year";
                output;
            %end;
            log_text = "observations in input dataset &INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year = &&obs_&year";
            output;
            log_text = "observations in output dataset &OUTPUT_DATASET_FOLDER..phrase_mentions_&year = &&new_obs_&year";
            output;
            log_text = "# terms mentioned but not mapped to a contextual phrase in &year = %sysfunc(strip(&&count_no_phrase_mapped_&year))";
            output;
        %end;
    run;

    data work.check_batch_log;
        infile "&LOG_FOLDER.MP_MINION_LOG.txt" truncover;
        input log $256.;
        length log_text $256.;
        if substr(log,1,14)="batch number =" then do;
            batch = substr(log,16,10);
            retain batch;
        end;
        if substr(log,1,28)="year                       =" then do;
            year = substr(log,30,4);
            retain year;
        end;
        if substr(log,1,28)="part                       =" then do;
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
/*            %if &LOG_FOLDER ~= %then %do;*/
                work.check_batch_log (keep = log_text)
/*            %end;*/
        ;
    run;

    title "check for errors";
        proc print 
            data=work.check_all noobs; 
        run;
    title;

    %put date MAPPING_PHRASES macro ended %sysfunc(date(),worddate.);
    %put time MAPPING_PHRASES macro ended %sysfunc(time(),time5.0);
%mend;
