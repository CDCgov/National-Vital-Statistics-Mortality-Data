/***************************************************************************************************************
PROGRAM NAME:       RECORD SELECTION - MASTER
VERSION:			CSP Update 2016 April 01
****************************************************************************************************************/

%macro RECORD_SELECTION_MASTER( FIRSTOBS=, 
                                OBS=, 
                                BATCHSIZE=, 
                                MAX_WINDOWS=,
                                SELECT_ON_ICD=, 
								SIMPLE_RANDOM_SAMPLE=,
                                SYSTEMATIC_SAMPLE=, 
                                SAMPLE_SHIFT=, 
                                BATCH_REDUX=,
                              );
	/***********************************************************************************************************
	                                                SECTION 1
	************************************************************************************************************/

    OPTIONS AUTOSIGNON;

    %put date RECORD_SELECTION_MASTER macro started %sysfunc(date(),worddate.);
    %put time RECORD_SELECTION_MASTER macro started %sysfunc(time(),time8.2);

	%let year_start				= %sysfunc(compress(&year_start))		;
	%let year_end				= %sysfunc(compress(&year_end))		 	;
	%let programpath			= %sysfunc(compress(%str(&programpath)));
	%let projectpath			= %sysfunc(compress(%str(&projectpath)));
	%let LOG_FOLDER				= %sysfunc(compress(%str(&LOG_FOLDER)))	;
	%let autoexec				= %sysfunc(compress(%str(&autoexec_folder)))	;

	%if &FIRSTOBS 				~= %then %let FIRSTOBS 				=%sysfunc(compress(&FIRSTOBS)) 				;
	%if &OBS 					~= %then %let OBS					=%sysfunc(compress(&OBS))					;
	%if &BATCHSIZE 				~= %then %let BATCHSIZE				=%sysfunc(compress(&BATCHSIZE))				;
	%if &MAX_WINDOWS 			~= %then %let MAX_WINDOWS			=%sysfunc(compress(&MAX_WINDOWS))			;
	%if &SELECT_ON_ICD 			~= %then %let SELECT_ON_ICD			=%sysfunc(compress(&SELECT_ON_ICD))			;
	%if &SIMPLE_RANDOM_SAMPLE 	~= %then %let SIMPLE_RANDOM_SAMPLE	=%sysfunc(compress(&SIMPLE_RANDOM_SAMPLE))	;
	%if &SYSTEMATIC_SAMPLE 		~= %then %let SYSTEMATIC_SAMPLE		=%sysfunc(compress(&SYSTEMATIC_SAMPLE))		;
	%if &SAMPLE_SHIFT 			~= %then %let SAMPLE_SHIFT			=%sysfunc(compress(&SAMPLE_SHIFT))			;
	%if &BATCH_REDUX 			~= %then %let BATCH_REDUX			=%sysfunc(compress(&BATCH_REDUX))			;

	/***********************************************************************************************************
	                                                SECTION 2
	************************************************************************************************************/

    %do year = &year_start %to &year_end;
		/*FIRSTOBS -> first_obs_year*/
        %if &FIRSTOBS = %then %do;
            %let firstobs_&year = 1;
            %put NOTE: For year &year, the first record read from raw data will be record 1.;
        %end;

        %else %do;
            %let firstobs_&year = &FIRSTOBS;
			%put NOTE: For year &year, the first record read from raw data will be record &&firstobs_&year.; 
        %end;

		/*OBS -> obs_year*/
        %if &OBS = %then %do;
            %let dsid       =   %sysfunc(open(data.selected_population_&year));
            %let obs_&year  =   %sysfunc(attrn(&dsid,nlobs));
            %let rc         =   %sysfunc(close(&dsid));
            %put NOTE: For year &year, the last record read from raw data will be record &&obs_&year.;
        %end;

        %else %do;
            %let dsid       =   %sysfunc(open(data.selected_population_&year));
            %let obs_&year  =   %sysfunc(attrn(&dsid,nlobs));
            %let rc         =   %sysfunc(close(&dsid));
			%if  &&obs_&year < &obs %then %let &obs = obs_&year;
			%let obs_&year 	= &obs;
            %put NOTE: For year &year, the last record read from raw data will be record &&obs_&year.;
        %end;

		/*firstobs_year and obs_year check*/
        %if &&obs_&year < &&firstobs_&year %then %do;
            %let firstobs_&year = 1;
            %put NOTE: FIRSTOBS > OBS.  For year &year, the first record read from input dataset will be record 1.;
        %end;

		/*BATCHSIZE -> batchsize_year*/
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
		%let MAX_WINDOWS = 1;
		%put NOTE: Maximum number of SAS windows to open for batch processing is &MAX_WINDOWS;
	%end;
	
	%else %do;
		%let MAX_WINDOWS 			= &MAX_WINDOWS;
		%put NOTE: Maximum number of SAS windows to open for batch processing is &MAX_WINDOWS;
	%end;

	/*SELECT_ON_ICD*/
	%let SELECT_ON_ICD 					= &SELECT_ON_ICD;
	%put NOTE: SELECT_ON_ICD 			= &SELECT_ON_ICD;

	/*SIMPLE_RANDOM_SAMPLE*/
	%let SIMPLE_RANDOM_SAMPLE 			= &SIMPLE_RANDOM_SAMPLE;
	%put NOTE: SIMPLE_RANDOM_SAMPLE 	= &SIMPLE_RANDOM_SAMPLE;

	/*SYSTEMATIC_SAMPLE*/
    %let SYSTEMATIC_SAMPLE 				= &SYSTEMATIC_SAMPLE;
	%put NOTE: SYSTEMATIC_SAMPLE 		= &SYSTEMATIC_SAMPLE;

	/*SAMPLE_SHIFT*/
	%let SAMPLE_SHIFT 					= &SAMPLE_SHIFT;
	%put NOTE: SAMPLE_SHIFT 				= &SAMPLE_SHIFT;

	/*LOG_FOLDER -> LOG_SCRIPT*/
	%let LOG_FOLDER						= %str(&LOG_FOLDER);
    %let LOG_SCRIPT						= %str(-log '&LOG_FOLDER.RS_log_&j..txt');
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
		%do part = 1 %to %sysfunc(ceil(%sysevalf((&&obs_&year - &&firstobs_&year + 1)/&&batchsize_&year)));
			%let b_counter = %sysevalf(&b_counter + 1);
			filename autoexec "&autoexec_folder.RS_&b_counter..sas";

			data _null_;
				file autoexec;
				put "%nrstr(%let) year                  = &year;";
				put "%nrstr(%let) part                  = &part;";
				put "%nrstr(%let) firstobs              = %sysevalf(&&firstobs_&year + (&part - 1)*&&batchsize_&year);";
				put "%nrstr(%let) obs                   = %sysevalf(&&firstobs_&year + &part * &&batchsize_&year - 1);";
				put "%nrstr(%let) select_on_ICD         = &select_on_ICD;";
				put "%nrstr(%let) systematic_sample     = &systematic_sample;";
				put "%nrstr(%let) sample_shift          = &sample_shift;";
				put "%nrstr(%let) log_folder     		= &log_folder;";
				put "%nrstr(%let) projectpath    		= &projectpath;";
				run;
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
	"sas 	-sysin '&ProgramPath\RECORD SELECTION - MINION.sas'
			-noicon	
			-nosplash 
			-autoexec '&autoexec_folder.RS_&j..sas'
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
			file "&LOG_FOLDER.RS_MINION_LOG.txt";
			put "Start of RS MINION LOG";
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
				infile "&LOG_FOLDER.RS_LOG_&j..txt";
				file "&LOG_FOLDER.RS_MINION_LOG.txt"
					mod
				;
				input;
				put _infile_;
			run;

			data _null_;
				infile "&LOG_FOLDER.RS_MINI_&j..txt";
				file "&LOG_FOLDER.RS_MINION_LOG.txt"
					mod
				;
				input;
				put _infile_;
			run;
			%let rc     = %sysfunc(filename(myRef,&LOG_FOLDER.RS_LOG_&j..txt));
			%let sysrc  = %sysfunc(fdelete(&myRef));
			%let rc     = %sysfunc(filename(myRef,&LOG_FOLDER.RS_MINI_&j..txt));
			%let sysrc  = %sysfunc(fdelete(&myRef));
		%end;
	%end;

	/***********************************************************************************************************
	                                                SECTION 6
	************************************************************************************************************/

	%put date RECORD_SELECTION_AUTOEXEC_SIGN_ON START %sysfunc(date(),worddate.);
	%put time RECORD_SELECTION_AUTOEXEC_SIGN_ON START %sysfunc(time(),time8.2);

	%include "\\cdc\csp_Project\CIPSEA_PII_DVS_MSB\Drug_Involved_Mortality\Final_Programs\2019-01-18\log_csp_signon.sas" /NOSOURCE2 ;
	%initialize_signon();

    OPTIONS AUTOSIGNON;
	%LET RCRS=%SYSFUNC(grdsvc_enable(_ALL_,resource=CSP_MNL )) ;
   	%PUT Return Code = &RCRS;
	signon cspRS wait=yes MACVAR=SIGNON_RC ;
		%log_signon(session_name=cspRS);

		%SYSLPUT year            		= &year			  	  	/remote=cspRS;
		%SYSLPUT batch_redux     		= &batch_redux        	/remote=cspRS;
		%SYSLPUT year_start      		= &year_start         	/remote=cspRS;
		%SYSLPUT year_end        		= &year_end           	/remote=cspRS;
		%SYSLPUT SELECT_ON_ICD      	= &SELECT_ON_ICD      	/remote=cspRS;
		%SYSLPUT SIMPLE_RANDOM_SAMPLE 	= &SIMPLE_RANDOM_SAMPLE /remote=cspRS;
		%SYSLPUT SYSTEMATIC_SAMPLE  	= &SYSTEMATIC_SAMPLE  	/remote=cspRS;
		%SYSLPUT SAMPLE_SHIFT       	= &SAMPLE_SHIFT       	/remote=cspRS;
		%SYSLPUT LOG_FOLDER  			= &LOG_FOLDER  			/remote=cspRS;
		%SYSLPUT MAX_WINDOWS       		= &MAX_WINDOWS        	/remote=cspRS;
		%do year = &year_start %to &year_end;
			%SYSLPUT obs_&year			= &&obs_&year        	/remote=cspRS;
			%SYSLPUT firstobs_&year  	= &&firstobs_&year   	/remote=cspRS;
			%SYSLPUT batchsize_&year	= &&batchsize_&year  	/remote=cspRS;
			%SYSLPUT batch_start_&year  = &&batch_start_&year   /remote=cspRS;
			%SYSLPUT batch_end_&year    = &&batch_end_&year     /remote=cspRS;
		%end;

		rsubmit cspRS wait=yes inheritlib=(data input raw results temp);
			proc Printto NEW LOG="&LOG_FOLDER.RS_MASTER.txt";

	%put date RECORD_SELECTION_AUTOEXEC_SIGN_ON DONE  %sysfunc(date(),worddate.);
	%put time RECORD_SELECTION_AUTOEXEC_SIGN_ON DONE %sysfunc(time(),time8.2);

			%macro append();
				%nrstr(%%)if &batch_redux = %nrstr(%%)then %nrstr(%%)do;
					%nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
						%nrstr(%%)if %sysfunc(exist(data.cleaned_records_&year)) %nrstr(%%)then %nrstr(%%)do;
							proc datasets 
								library = data
								nolist;
								delete cleaned_records_&year;
							quit;
						%nrstr(%%)end;

						%nrstr(%%)do part = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf((&&obs_&year - &&firstobs_&year + 1)/&&batchsize_&year)));
							proc append
								base = data.cleaned_records_&year
								data = temp.cleaned_records_&year._&part
								force;
							quit;

							%nrstr(%%)if &part = 1 %nrstr(%%)then %nrstr(%%)do;
								data data.cleaned_records_&year (compress = yes);
									set data.cleaned_records_&year;
								run;
							%nrstr(%%)end;
						%nrstr(%%)end;

						proc sql noprint;
							create table data.cleaned_records_&year (compress = yes) as
							select 	distinct uniq_id,
									chain,
									descr_lin5,
									inj_descr,
									cleaned_chain,
									cleaned_descr_lin5,
									cleaned_inj_descr
								from data.cleaned_records_&year
								order by uniq_id;
						quit;

						%nrstr(%%)if &SIMPLE_RANDOM_SAMPLE>0 %nrstr(%%)then %nrstr(%%)do;
							proc surveyselect
								data 	= data.cleaned_records_&year 
								out 	= data.cleaned_records_&year (compress = yes)
								method 	= SRS
								sampsize= &SIMPLE_RANDOM_SAMPLE
							;
							run;
						%nrstr(%%)end;
					%nrstr(%%)end;

					%nrstr(%%)do year = &year_start %nrstr(%%)to &year_end;
						proc datasets 
							library = temp
							nolist;
							delete
							%nrstr(%%)do part = 1 %nrstr(%%)to %sysfunc(ceil(%sysevalf((&&obs_&year - &&firstobs_&year + 1)/&&batchsize_&year)));
								selected_records_&year._&part
								cleaned_records_&year._&part
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
								base = data.cleaned_records_&year
								data = temp.cleaned_records_&year._&part
								force;
							quit;

							proc sql noprint;
								create table data.cleaned_records_&year (compress = yes) as
								select  distinct uniq_id,
										chain,
										descr_lin5,
										inj_descr,
										cleaned_chain,
										cleaned_descr_lin5,
										cleaned_inj_descr
								from data.cleaned_records_&year
								order by uniq_id;
							quit;

							proc datasets 
								library = temp
								nolist;
								delete
								selected_records_&year._&part
								cleaned_records_&year._&part
							;
							quit;
						%nrstr(%%)end;
					%nrstr(%%)end;
					%nrstr(%%)let batch_end_&year = &b_counter;
					%nrstr(%%)if &SIMPLE_RANDOM_SAMPLE>0 %nrstr(%%)then %nrstr(%%)do;
						proc surveyselect
							data 	= data.cleaned_records_&year 
							out 	= data.cleaned_records_&year (compress = yes)
							method 	= SRS
							sampsize= &SIMPLE_RANDOM_SAMPLE
						;
						run;
					%nrstr(%%)end;
				%nrstr(%%)end;
			%mend append;

			%append();

		endrsubmit;
	signoff cspRS;

	/***********************************************************************************************************
	                                                SECTION 7
	************************************************************************************************************/

    %do year = &year_start %to &year_end;
        %let dsid           =   %sysfunc(open(data.selected_population_&year));
        %let obs_&year  	=   %sysfunc(attrn(&dsid,nlobs));
        %let rc             =   %sysfunc(close(&dsid));
    %end;

    %do year = &year_start %to &year_end;
        %let dsid           =   %sysfunc(open(data.cleaned_records_&year));
        %let new_obs_&year  =   %sysfunc(attrn(&dsid,nlobs));
        %let rc             =   %sysfunc(close(&dsid));
    %end;

    data check_data;
        length log_text $256.;
        %do year = &year_start %to &year_end;
            log_text = "observations in raw data for year &year    = &&obs_&year";
            output;
            log_text = "observations in data.cleaned_records_&year = &&new_obs_&year";
            output;
        %end;
    run;

    data work.check_batch_log;
        infile "&LOG_FOLDER.RS_MINION_LOG.txt" truncover;
        input log $256.;
        length log_text $256.;
        if substr(log,1,14)="batch number =" then do;
            batch = substr(log,16,10);
            retain batch;
        end;
        if substr(log,1,20)="year               =" then do;
            year = substr(log,22,4);
            retain year;
        end;
        if substr(log,1,20)="part               =" then do;
            part = substr(log,22,10);
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

    %put date RECORD_SELECTION_MASTER macro ended %sysfunc(date(),worddate.);
    %put time RECORD_SELECTION_MASTER macro ended %sysfunc(time(),time8.2);
%mend;
