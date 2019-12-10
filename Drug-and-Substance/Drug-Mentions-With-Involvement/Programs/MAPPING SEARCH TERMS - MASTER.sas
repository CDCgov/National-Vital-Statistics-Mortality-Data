/***************************************************************************************************************
PROGRAM NAME:       MAPPING SEARCH TERMS - MASTER
VERSION:			CSP Update 2016 April 01
****************************************************************************************************************/

%macro MAPPING_SEARCH_TERMS_MASTER( INPUT_DATASET_FOLDER=,
                                    INPUT_DATASET_FILE=,
                                    OUTPUT_DATASET_FOLDER=,
                                    APPEND_DATASET_FOLDER=,
                                    APPEND=,
                                    INPUT_SEARCH_TERM_DATASET=,
                                    BATCHSIZE=, 
                                    MAX_WINDOWS=,
                                    SEARCH_TERM_BATCHSIZE=, 
                                    BATCH_REDUX=
                                  );

	/***********************************************************************************************************
	                                                SECTION 1
	************************************************************************************************************/

    %put date MAPPING_SEARCH_TERMS_MASTER macro started %sysfunc(date(),worddate.);
    %put time MAPPING_SEARCH_TERMS_MASTER macro started %sysfunc(time(),time5.0);

	%let year_start				= %sysfunc(compress(&year_start))		;
	%let year_end				= %sysfunc(compress(&year_end))		 	;
	%let programpath			= %sysfunc(compress(%str(&programpath)));
	%let projectpath			= %sysfunc(compress(%str(&projectpath)));
	%let LOG_FOLDER				= %sysfunc(compress(%str(&LOG_FOLDER)))	;
	%let autoexec				= %sysfunc(compress(%str(&autoexec_folder)))	;

	%if &INPUT_DATASET_FOLDER 		~= %then %let INPUT_DATASET_FOLDER 		=%sysfunc(compress(&INPUT_DATASET_FOLDER)) 		;
	%if &INPUT_DATASET_FILE 		~= %then %let INPUT_DATASET_FILE		=%sysfunc(compress(&INPUT_DATASET_FILE))		;
	%if &OUTPUT_DATASET_FOLDER 		~= %then %let OUTPUT_DATASET_FOLDER		=%sysfunc(compress(&OUTPUT_DATASET_FOLDER))		;
	%if &APPEND_DATASET_FOLDER 		~= %then %let APPEND_DATASET_FOLDER		=%sysfunc(compress(&APPEND_DATASET_FOLDER))		;
	%if &APPEND 					~= %then %let APPEND					=%sysfunc(compress(&APPEND))					;
	%if &INPUT_SEARCH_TERM_DATASET 	~= %then %let INPUT_SEARCH_TERM_DATASET	=%sysfunc(compress(&INPUT_SEARCH_TERM_DATASET))	;
	%if &BATCHSIZE 					~= %then %let BATCHSIZE					=%sysfunc(compress(&BATCHSIZE))					;
	%if &MAX_WINDOWS 				~= %then %let MAX_WINDOWS				=%sysfunc(compress(&MAX_WINDOWS))				;
	%if &SEARCH_TERM_BATCHSIZE 		~= %then %let SEARCH_TERM_BATCHSIZE		=%sysfunc(compress(&SEARCH_TERM_BATCHSIZE))		;
	%if &BATCH_REDUX 				~= %then %let BATCH_REDUX				=%sysfunc(compress(&BATCH_REDUX))				;

	/***********************************************************************************************************
	                                                SECTION 2
	************************************************************************************************************/

	/*INPUT_DATASET_FOLDER*/
	%let INPUT_DATASET_FOLDER 	= &INPUT_DATASET_FOLDER;
	%put NOTE: Records to be processed will come from &INPUT_DATASET_FOLDER folder.;

	/*INPUT_DATASET_FILE*/
	%let INPUT_DATASET_FILE		= &INPUT_DATASET_FILE;
    %put NOTE: File containing records to be processed will be the &INPUT_DATASET_FILE file.;

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
			%let INPUT_DATASET_FILE 	= cleaned_records;
            %put NOTE: File containing records to be processed will be the %str(data.cleaned_records_&year) file.;

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

	/*Check APPEND*/
    %if &APPEND=1 %then %do year = &year_start %to &year_end;
        %if %sysfunc(exist(&APPEND_DATASET_FOLDER..mentions_&year)) %then %do;
            %let DSID = %sysfunc(open(&APPEND_DATASET_FOLDER..mentions_&year, IS));
            %let anobs = %sysfunc(attrn(&DSID, ANOBS));
            %let whstmt = %sysfunc(attrn(&DSID, WHSTMT));
            %if &anobs = 1 & &whstmt = 0 %then %do;
                %let obs_append_&year = %sysfunc(attrn(&DSID, NLOBS));
            %end;
            %let DSID = %sysfunc(close(1));
        %end;
        %else %do;
            %let APPEND = ;
            %put NOTE: &APPEND_DATASET_FOLDER..mentions_&year does not exist and will not be appended.;
        %end;
    %end;
    %else %do;
        %let APPEND = &APPEND;
		%put NOTE: Data set will be appended if it exists;
    %end;

	/*OUTPUT_DATASET_FOLDER*/
    %let OUTPUT_DATASET_FOLDER = &OUTPUT_DATASET_FOLDER;
	%put NOTE: The processed data will be in the &OUTPUT_DATASET_FOLDER folder;

	/*INPUT_SEARCH_TERM_DATASET*/
    %let INPUT_SEARCH_TERM_DATASET = &INPUT_SEARCH_TERM_DATASET;
	%put NOTE: The input data will be &INPUT_SEARCH_TERM_DATASET;

	/*count_search_term*/
	/*length_search_term*/
    proc sql noprint;
        select count(search_term)
            into :count_search_term
            from input.&INPUT_SEARCH_TERM_DATASET;
        select max(length_search_term)
            into :length_search_term
            from
                (select lengthn(search_term) as length_search_term
                    from input.&INPUT_SEARCH_TERM_DATASET
                );
    quit;
	%put NOTE: The number of search terms to be identified is &count_search_term;
	%put NOTE: The max length of search terms to be identified is &length_search_term;

	/*search_term_batchsize*/	
    %if &SEARCH_TERM_BATCHSIZE = %then %do;
        %let SEARCH_TERM_BATCHSIZE = &count_search_term;
        %put NOTE: All search terms will be searched for each batch;
    %end;

    %else %do;
        %let SEARCH_TERM_BATCHSIZE = &SEARCH_TERM_BATCHSIZE;
        %put NOTE: &SEARCH_TERM_BATCHSIZE search terms will be searched for at a time;
    %end;

	/*LOG_FOLDER -> LOG_SCRIPT*/
	%let LOG_FOLDER						= %str(&LOG_FOLDER);
    %let LOG_SCRIPT						= %str(-log '&LOG_FOLDER.MST_log_&j..txt');
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
			filename autoexec "&autoexec_folder.MST_&b_counter..sas";

			data _null_;
				file autoexec;
				put "%nrstr(%let) year                      = &year;";
				put "%nrstr(%let) part                      = &part;";
				put "%nrstr(%let) firstobs                  = %sysevalf(&&firstobs_&year + (&part - 1)*&&BATCHSIZE_&year);";
				put "%nrstr(%let) obs                       = %sysevalf(&part * &&BATCHSIZE_&year);";
				put "%nrstr(%let) input_dataset_folder      = &INPUT_DATASET_FOLDER;";
				put "%nrstr(%let) input_dataset_file        = &INPUT_DATASET_FILE;";
				put "%nrstr(%let) input_search_term_dataset = &INPUT_SEARCH_TERM_DATASET;";
				put "%nrstr(%let) count_search_term         = &count_search_term;";
				put "%nrstr(%let) search_term_batchsize     = &SEARCH_TERM_BATCHSIZE;";
				put "%nrstr(%let) length_search_term        = &length_search_term;";
				put "%nrstr(%let) log_folder     			= &log_folder;";
				put "%nrstr(%let) projectpath    			= &projectpath;";
			run;
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
	"sas 	-sysin '&ProgramPath\MAPPING SEARCH TERMS - MINION.sas' 
			-noicon 
			-nosplash 
			-autoexec '&autoexec_folder.MST_&j..sas'
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
			file "&LOG_FOLDER.MST_MINION_LOG.txt";
			put "Start of MST MINION LOG";
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
				infile "&LOG_FOLDER.MST_LOG_&j..txt";
				file "&LOG_FOLDER.MST_MINION_LOG.txt"
					mod
				;
				input;
				put _infile_;
			run;

			data _null_;
				infile "&LOG_FOLDER.MST_MINI_&j..txt";
				file "&LOG_FOLDER.MST_MINION_LOG.txt"
					mod
				;
				input;
				put _infile_;
			run;
			%let rc     = %sysfunc(filename(myRef,&LOG_FOLDER.MST_LOG_&j..txt));
			%let sysrc  = %sysfunc(fdelete(&myRef));
			%let rc     = %sysfunc(filename(myRef,&LOG_FOLDER.MST_MINI_&j..txt));
			%let sysrc  = %sysfunc(fdelete(&myRef));
		%end;
	%end;

	/***********************************************************************************************************
	                                                SECTION 6
	************************************************************************************************************/

	%put date MAPPING_SEARCH_TERMS_MASTER_SIGN_ON START %sysfunc(date(),worddate.);
	%put time MAPPING_SEARCH_TERMS_MASTER_SIGN_ON START %sysfunc(time(),time8.2);

	%include "\\cdc\csp_Project\CIPSEA_PII_DVS_MSB\Drug_Involved_Mortality\Final_Programs\2019-01-18\log_csp_signon.sas" /NOSOURCE2 ;
	%initialize_signon();

	OPTIONS AUTOSIGNON;
	%LET RCMST=%SYSFUNC(grdsvc_enable(_ALL_,resource=CSP_MNL )) ;
   	%PUT Return Code = &RCMST;
	signon cspMST wait=yes MACVAR=SIGNON_RC ;
		%log_signon(session_name=cspMST);

		%SYSLPUT year            		= &year			  			/remote=cspMST;
		%SYSLPUT batch_redux     		= &batch_redux       		/remote=cspMST;
		%SYSLPUT year_start      		= &year_start        		/remote=cspMST;
		%SYSLPUT year_end        		= &year_end          		/remote=cspMST;
		%SYSLPUT b_counter       		= &b_counter         		/remote=cspMST;
		%SYSLPUT APPEND  				= &APPEND  					/remote=cspMST;
		%SYSLPUT APPEND_DATASET_FOLDER  = &APPEND_DATASET_FOLDER  	/remote=cspMST;
		%SYSLPUT OUTPUT_DATASET_FOLDER  = &OUTPUT_DATASET_FOLDER  	/remote=cspMST;
		%SYSLPUT search_term_batchsize  = &search_term_batchsize  	/remote=cspMST;
		%SYSLPUT COUNT_SEARCH_TERM  	= &COUNT_SEARCH_TERM  		/remote=cspMST;
		%SYSLPUT LOG_FOLDER  			= &LOG_FOLDER  				/remote=cspMST;
		%SYSLPUT projectpath    		= &projectpath  			/remote=cspMST;
		%do year = &year_start %to &year_end;
			%SYSLPUT obs_&year       	= &&obs_&year        		/remote=cspMST;
			%SYSLPUT firstobs_&year  	= &&firstobs_&year   		/remote=cspMST;
			%SYSLPUT batchsize_&year 	= &&batchsize_&year  		/remote=cspMST;
		%end;

		rsubmit cspMST wait=yes inheritlib=(data input raw results temp);
			proc Printto NEW LOG="&LOG_FOLDER.MST_MASTER.txt";

	%put date MAPPING_SEARCH_TERMS_MASTER_SIGN_ON DONE %sysfunc(date(),worddate.);
	%put time MAPPING_SEARCH_TERMS_MASTER_SIGN_ON DONE %sysfunc(time(),time8.2);

			%macro append();
				%nrstr(%%)if &batch_redux = %nrstr(%%)then %nrstr(%%)do;
					%nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
						%nrstr(%%)if &APPEND = 1 and &APPEND_DATASET_FOLDER ~= &OUTPUT_DATASET_FOLDER %nrstr(%%)then %nrstr(%%)do;
							data &OUTPUT_DATASET_FOLDER..mentions_&year;
								set &APPEND_DATASET_FOLDER..mentions_&year;
							run;
						%nrstr(%%)end;

						%nrstr(%%)else %nrstr(%%)if &APPEND ~=1 %nrstr(%%)then %nrstr(%%)do;
							%nrstr(%%)if %sysfunc(exist(&OUTPUT_DATASET_FOLDER..mentions_&year)) %nrstr(%%)then %nrstr(%%)do;
								proc datasets 
									library = &OUTPUT_DATASET_FOLDER
									nolist;
									delete mentions_&year;
								quit;
							%nrstr(%%)end;
						%nrstr(%%)end;

						%nrstr(%%)let merged_files = 0;

						%nrstr(%%)do part = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf((&&obs_&year - &&firstobs_&year + 1)/&&BATCHSIZE_&year)));
							%nrstr(%%)do search_term_batch = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf(&count_search_term / &search_term_batchsize)));
								%nrstr(%%)let DSID = %sysfunc(open(temp.mentions_&year._&part._&search_term_batch, IS));
								%nrstr(%%)if &DSID ~= 0 %nrstr(%%)then %nrstr(%%)do;
									%nrstr(%%)let anobs = %sysfunc(attrn(&DSID, ANOBS));
									%nrstr(%%)let whstmt = %sysfunc(attrn(&DSID, WHSTMT));
									%nrstr(%%)if &anobs = 1 & &whstmt = 0 %nrstr(%%)then %nrstr(%%)do;
										%nrstr(%%)let counted = %sysfunc(attrn(&DSID, NLOBS));
										%nrstr(%%)let merged_files = %sysevalf(&merged_files + 1);
									%nrstr(%%)end;
									%nrstr(%%)let DSID = %sysfunc(close(1));
								%nrstr(%%)end;

								%nrstr(%%)if &counted > 0 %nrstr(%%)then %nrstr(%%)do;
									proc append
										base = &OUTPUT_DATASET_FOLDER..mentions_&year
										data = temp.mentions_&year._&part._&search_term_batch
										force;
									quit;

									%nrstr(%%)if &merged_files = 1 %nrstr(%%)then %nrstr(%%)do;
										data &OUTPUT_DATASET_FOLDER..mentions_&year (compress = yes);
											set &OUTPUT_DATASET_FOLDER..mentions_&year;
										run;
									%nrstr(%%)end;
								%nrstr(%%)end;
							%nrstr(%%)end;
						%nrstr(%%)end;

						proc sql noprint;
							create table &OUTPUT_DATASET_FOLDER..mentions_&year (compress=yes) as
							select  distinct    uniq_ID,
									search_term,
									text_field,
									chain,
									cleaned_chain,
									descr_lin5,
									cleaned_descr_lin5,
									inj_descr,
									cleaned_inj_descr,
									term_position_beg,
									term_position_end
								from &OUTPUT_DATASET_FOLDER..mentions_&year
								order by    uniq_ID,
											text_field, 
											term_position_beg, 
											term_position_end desc;
						quit;
					%nrstr(%%)end;

					%nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
						proc datasets 
							library = temp
							nolist;
							delete 
							%nrstr(%%)do part = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf((&&obs_&year - &&firstobs_&year + 1)/&&BATCHSIZE_&year)));
								%nrstr(%%)do search_term_batch = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf(&count_search_term / &search_term_batchsize)));
										mentions_&year._&part._&search_term_batch
								%nrstr(%%)end;
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
					%nrstr(%%)do part = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf((&&obs_&year - &&firstobs_&year + 1)/&&BATCHSIZE_&year))); 
						%nrstr(%%)let b_counter = %sysevalf(&b_counter + 1);
						%nrstr(%%)if &batch_redux = &b_counter %nrstr(%%)then %nrstr(%%)do;
							%nrstr(%%)do search_term_batch = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf(&count_search_term / &search_term_batchsize)));
								%nrstr(%%)let DSID = %sysfunc(open(temp.mentions_&year._&part._&search_term_batch, IS));
								%nrstr(%%)if &DSID ~= 0 %nrstr(%%)then %nrstr(%%)do;
									%nrstr(%%)let anobs = %sysfunc(attrn(&DSID, ANOBS));
									%nrstr(%%)let whstmt = %sysfunc(attrn(&DSID, WHSTMT));
									%nrstr(%%)if &anobs = 1 & &whstmt = 0 %nrstr(%%)then %nrstr(%%)do;
										%nrstr(%%)let counted = %sysfunc(attrn(&DSID, NLOBS));
									%nrstr(%%)end;
								%nrstr(%%)let DSID = %sysfunc(close(1));
								%nrstr(%%)end;

								%nrstr(%%)if &counted > 0 %nrstr(%%)then %nrstr(%%)do;
									proc append
										base = &OUTPUT_DATASET_FOLDER..mentions_&year
										data = temp.mentions_&year._&part._&search_term_batch
										force;
									quit;
								%nrstr(%%)end;
							%nrstr(%%)end;

							proc sql noprint;
								create table &OUTPUT_DATASET_FOLDER..mentions_&year (compress=yes) as
								select  distinct    uniq_ID,
										search_term,
										text_field,
										chain,
										cleaned_chain,
										descr_lin5,
										cleaned_descr_lin5,
										inj_descr,
										cleaned_inj_descr,
										term_position_beg,
										term_position_end
									from &OUTPUT_DATASET_FOLDER..mentions_&year
									order by    uniq_ID,
										text_field, 
										term_position_beg, 
										term_position_end desc;
							quit;

							proc datasets 
								library = temp
								nolist;
								delete 
								%nrstr(%%)do search_term_batch = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf(&count_search_term / &search_term_batchsize)));
									mentions_&year._&part._&search_term_batch
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

			%macro term_overlap();
				%nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
					data &OUTPUT_DATASET_FOLDER..mentions_&year (compress = yes);
						set &OUTPUT_DATASET_FOLDER..mentions_&year;
						by uniq_ID text_field term_position_beg descending term_position_end;
						retain check_position_beg;
						retain check_position_end;
						if first.text_field then do;
							check_position_beg = term_position_beg;
							check_position_end = term_position_end;
							overlap = 'first term or no overlap';
						end;
						else do;
							if  term_position_beg >= check_position_beg and 
								term_position_end <= check_position_end 
								then overlap = 'total overlap';
							else if check_position_beg <= term_position_beg <= check_position_end and 
								term_position_end > check_position_end 
								then overlap = 'partial overlap';
							else if term_position_beg > check_position_end then do;
								check_position_beg = term_position_beg;
								check_position_end = term_position_end;
								overlap = 'first term or no overlap';
							end;
						end;
					run;

					proc sql;
						create table temp.check_search_terms_&year as
						select  &year as year,
								a.uniq_ID, 
								a.search_term, 
								b.overlapping_term,
								cat(substr(a.search_term, 1, b.term_position_beg - a.term_position_beg),
								b.overlapping_term) as potential_search_term
							from
								(select uniq_ID,
										text_field,
										search_term,
										term_position_beg,
										term_position_end
									from &OUTPUT_DATASET_FOLDER..mentions_&year
									where overlap = 'first term or no overlap'
								) as a
								inner join 
								(select uniq_ID,
										text_field,
										search_term as overlapping_term,
										term_position_beg,
										term_position_end
									from &OUTPUT_DATASET_FOLDER..mentions_&year
									where overlap = 'partial overlap'
								) as b
							on  a.uniq_ID = b.uniq_ID and 
								a.text_field = b.text_field and
								a.term_position_beg <= b.term_position_beg <= a.term_position_end and 
								a.term_position_end < b.term_position_end
							order by 	a.uniq_ID,
										a.search_term,
										b.overlapping_term
						;
					quit;
				%nrstr(%%)end;

				proc sql;
					create table temp.check_search_terms as
					select *
						from
							%nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
								%nrstr(%%)if &year ~= &year_start %nrstr(%%)then %nrstr(%%)do;
									union corr
								%nrstr(%%)end;
								(select *
									from temp.check_search_terms_&year
								)
							%nrstr(%%)end;
					;
				quit;

			    proc export data = temp.check_search_terms
				    outfile = "&ProjectPath\check_search_terms.xls"
				    dbms = excelcs label replace;
				    SHEET='Check_Search_Terms';
			    run;

				%nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
					proc datasets 
						library = temp
						nolist;
						delete check_search_terms_&year;
					quit;
				%nrstr(%%)end;

				proc datasets 
					library = temp
					nolist;
					delete check_search_terms;
				quit;

	/***********************************************************************************************************
	                                                SECTION 7
	************************************************************************************************************/

				%nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
					proc sql;
						create table &OUTPUT_DATASET_FOLDER..mentions_&year
							(compress = yes
								drop = 	check_position_beg
								        check_position_end
								        overlap
							)    
							as 
						select  *
							from &OUTPUT_DATASET_FOLDER..mentions_&year
							where overlap = 'first term or no overlap'
							order by uniq_ID, text_field, term_position_beg;
					quit;
				%nrstr(%%)end;
			%mend term_overlap;

			%term_overlap();
		endrsubmit;
	signoff cspMST;

	/***********************************************************************************************************
	                                                SECTION 8
	************************************************************************************************************/

    %do year = &year_start %to &year_end;
        %let dsid           =   %sysfunc(open(&OUTPUT_DATASET_FOLDER..mentions_&year));
        %let new_obs_&year  =   %sysfunc(attrn(&dsid,nlobs));
        %let rc             =   %sysfunc(close(&dsid));
    %end;

    data check_data;
        length log_text $256.;
        %do year = &year_start %to &year_end;
            log_text = "observations in raw data for year &year = &&obs_&year";
            output;
            %if &APPEND = 1 %then %do;
                log_text = "original number of observations in &APPEND_DATASET_FOLDER..mentions_&year = &&obs_append_&year";
                output;
            %end;
            log_text = "observations in &OUTPUT_DATASET_FOLDER..mentions_&year = &&new_obs_&year";
            output;
        %end;
    run;

    data work.check_batch_log;
        infile "&LOG_FOLDER.MST_MINION_LOG.txt" truncover;
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
            work.check_batch_log (keep = log_text)
        ;
    run;

    title "check for errors";
        proc print 
            data=work.check_all noobs; 
        run;
    title;

    %put date MAPPING_SEARCH_TERMS_MASTER macro ended %sysfunc(date(),worddate.);
    %put time MAPPING_SEARCH_TERMS_MASTER macro ended %sysfunc(time(),time5.0);
%mend;
