var util = require("util"),
		Client = require("./client");
		
util.inherits(Saint, Client);
		
function Saint(username, sharedSecret, environment, options){
	this.defaultWaitTime = 5;
	this.waitTime = (options && options.waitTime) ? options.waitTime : this.defaultWaitTime;
	this.methods = {
		exportCreateJob: 'Saint.ExportCreateJob',
		exportGetStatus: 'Saint.CheckJobStatus',
		exportGetFileSegment: 'Saint.ExportGetFileSegment',
		importCreateJob: 'Classifications.CreateImport',
		importPopulateJob: 'Classifications.PopulateImport',
		importCommitJob: 'Classifications.CommitImport'
	}

	Client.apply(this, arguments);
}
var p = Saint.prototype;
p.clientRequest = Client.prototype.request;
p.getJob = function(parameters, callback){
	var self = this;
	this.clientRequest(this.methods.exportCreateJob, parameters, function(err, data){
		if(err){ callback(new Error(err.message)); }
		if(data.errors){
			callback(new Error(data.errors[0]));
		}else{
			self.getQueuedJob(data, callback);
		}
	});
}

p.getQueuedJob = function(jobId, callback){
	this.logger("info", "Getting Queued Job");
	var self = this,
			jobData = {"job_id" : jobId };
	this.clientRequest(this.methods.exportGetStatus, jobData, function(err, data){
		if(err){ 
			callback(err, data); 
		}else{
			var json = JSON.parse(data);

			if(json[0].status.toLowerCase() == 'completed' && json[1] && json[1].status.toLowerCase() == 'ready'){
				if(parseInt(json[1].viewable_pages,10) > 0){
					callback(null, json);
				}else{
					callback(new Error("There were no viable pages for SAINT job "+jobId));
				}
			}else if(json.status == "failed"){
				callback(new Error(json.status+": "+json.error_msg));
			}else{
				self.logger("info", "Job: "+jobId+" not ready yet");
				setTimeout(function(){
					self.logger("info", "Checking job: "+jobId+" status");
					self.getQueuedJob(jobId, callback);
				}, self.waitTime * 1000)
			}
		}
	});
};

p.getJobSegment = function(fileId, pageNum, callback){
	var self = this,
			segmentData = {"file_id": fileId, "segment_id": pageNum }
	this.clientRequest(this.methods.exportGetFileSegment, segmentData, function(err, data){
		if(err){ callback(new Error(err.message)); };
		var json = JSON.parse(data);
		if(json.length > 0){
			json = json[0];
			if(json.header == null || json.data == null){
				callback(new Error('No data was returned: ' + data));
			}else{
				callback(null, json);
			}
		}else{
			callback(new Error('Malformed formatted data response: '+ data));
		}
	});
};

p.importJob = function(jobDescription, jobData, callback){
	var self = this,
			populateData =[];
	while(jobData.length){
		populateData.push(jobData.splice(0,10000));
	}

	self.logger("info", "Creating Job");
  self.logger("info", jobDescription);
	self.logger("info", 'jobDescription: ' + JSON.stringify(jobDescription));
	this.importCreateJob(jobDescription, myFunction);

	function myFunction(err, data){
		self.logger("info", 'Adobe create job results(err): ' + err);
		self.logger("info", 'Adobe create job results(data): ' + data);
		if(err){ callback(new Error(err.message)); return; };
		var jobId = data;
		self.logger("Populating Job "+jobId);
		populateData.forEach(function(item, index){
			var reportData = {
				job_id: jobId,
				page: (index + 1),
				rows: item
			};
			self.logger("info", 'importing ', reportData);
			self.importPopulateJob(reportData, function(err, data){
				self.logger("info", "Populated page "+ index);
				if(err){
					callback(new Error(err.message));
				}else{
					if(index == populateData.length - 1){
						self.logger("info", "Committing job " + jobId);
						self.importCommitJob(jobId, function(err, data){
							if(err){ callback(new Error(err.message)); }
							callback(null, data);
						});
					}
				}
			});
		});
	}
};

p.importCreateJob = function(jobDescription, callback) {
	var self = this;
	this.request(this.methods.importCreateJob, jobDescription, doesSomething);

	function doesSomething(err, data) {
		self.logger("info", "Adobe real response: " + data);
		response = self.normalizeResponse(err, data, "failed to create import job");
		if (response.error.message) {
			callback(new Error(response.error.message));
		} else {
			callback(null, json.job_id);
		}
	}
}

p.importPopulateJob = function(populateData, callback){
	var self = this;
	this.request(this.methods.importPopulateJob, populateData, function(err, data){
		response = this.normalizeResponse(err, data, 'Failed to populate job: ' + jobId);
		if (response.error.message) {
			callback (new Error(response.error.message));
		} else{
			callback(null, response.json.job_id);
		}
	});
}

p.importCommitJob = function(jobId, callback){
	var self = this;
	this.request(this.methods.importCommitJob, {job_id: jobId}, function(err, data){
	response = this.normalizeResponse(err, data, "SAINT JOB: "+jobId+" failed to import");
	if (response.error.message) {
		callback(new Error(response.error.message));
	} else {
		callback(null, jobId);
		}
	});
}

p.normalizeResponse = function(err, data, defaultErrorMessage) {
	var response = {
		json: null,
		error: {
			message: null
		}
	}

	if (err && err.message) {
		response.error.message = err.message;
		return response;
	} else if (err) {
		response.error.message = err;
		return response;
	}

	try {
		var json = JSON.parse(data);
	} catch(exception) {
		response.error.message = defaultErrorMessage;
		response.error.trace = exception.trace();
		return response;
	}

	var hasErrorInsteadOfData = data.toLowerCase() == 'failed';
	var hasErrorsArrayInJson = 'erros' in json;
	var hasErrorStringInJson = 'error' in json;
	var hasErrorDescriptionInJson = 'error_description' in json;
	if(hasErrorInsteadOfData || hasErrorsArrayInJson || hasErrorStringInJson) {
		if (hasErrorsArrayInJson) {
			response.error.message = json.errors[0];
		} else if (hasErrorDescriptionInJson) {
			response.error.message = json.error_description;
		} else if (hasErrorStringInJson) {
			resonse.error.message = json.error;
		}
	} else {
		response.json = json;
	}
	return response;
}


module.exports = Saint;
