
import React from 'react';
import axios from 'axios';
import SurveyComponent from './SurveyQuestions.js';
import PlaceSurvey from './PlaceQuestions.js';
import ChildSurvey from './ChildQuestions.js';
import HealthSurvey from './HealthQuestions.js';

class Queue {
  constructor() {
    this.elements = {};
    this.head = 0;
    this.tail = 0;
    this.maxSize=4;
  }
  enqueue(element) {
    this.elements[this.tail] = element;
    this.tail++;
  }
  dequeue() {
    const item = this.elements[this.head];
    delete this.elements[this.head];
    this.head++;
    return item;
  }
  peek(peakIndex) {
    return this.elements[peakIndex];
  }
  getLength() {
    return this.tail - this.head;
  }
  isEmpty() {
    return this.length === 0;
  }
}

class App extends React.Component {

  

	constructor(props) {
  super(props);
  this.state = {
    twitterId:0,
    twitterText:"this is the test text for twitter",
    picLoc:"",
    userName: "",
    enteredName: false,
    prevSubmit: new Queue(),
    queueIndex:-1,
    exercise:"Training",
    searchId:0,
    qa:'none'
  }
  this.handleNameChange = this.handleNameChange.bind(this);
  this.updateSearchId = this.updateSearchId.bind(this);
  this.enteredName = this.enteredName.bind(this);
  this.getPrevSubmit = this.getPrevSubmit.bind(this);
  this.getNextSubmit = this.getNextSubmit.bind(this);
  this.onChangeExercise = this.onChangeExercise.bind(this);
  this.searchTweet = this.searchTweet.bind(this);
  this.skipTweet = this.skipTweet.bind(this);

}

enteredName(event) {
  if(this.state.userName==="") {
    return
  }
  this.setState({
    enteredName:true
  })
  if(this.state.exercise==='Practice') {
      this.onSearchSubmit(300) //first sample number used for practice
  }
  else {
    this.onSearchSubmit(0)
  }
}

handleNameChange(event) {
  this.setState({userName: event.target.value});
}

updateSearchId(event) {
  this.setState({searchId: event.target.value});
}

getPrevSubmit(event) {
  if(this.state.prevSubmit.isEmpty() || this.state.queueIndex===this.state.prevSubmit.head) {
    return;
  }

  let sampleNum = this.state.prevSubmit.peek(this.state.queueIndex-1)
  this.setState({
    queueIndex: this.state.queueIndex-2
  })
  this.onSearchSubmit(sampleNum,true);
}

getNextSubmit(event) {
  if(this.state.queueIndex===this.state.prevSubmit.tail-1) {
    return
  }
  else {
    let sampleNum = this.state.prevSubmit.peek(this.state.queueIndex+1)
    this.onSearchSubmit(sampleNum,true);
  }
}

skipTweet(event) {

  if(this.state.queueIndex===this.state.prevSubmit.tail-1) {
    this.onSearchSubmit(this.state.twitterId+1);
  }
  else {
    let sampleNum = this.state.prevSubmit.peek(this.state.queueIndex+1)
    this.onSearchSubmit(sampleNum,true);
  }
}

onChangeExercise(event) {
  this.setState({
    exercise:event.target.value
  })
}

searchTweet(event) {
  console.log(this.state.searchId)
  this.onSearchSubmit(this.state.searchId)
}

render = () => {

  if(this.state.exercise ==='Place' & this.state.enteredName===true) {
    return(
      <div>
      <h2>ASPIRE Social Media Labeling (version Sep 15th, 2022)</h2>
      <fieldset>
         <label>
           <p className='filler'>Please enter your user id in the text box below </p>
           <input name="name"  type="text"value = {this.state.userName} onChange={this.handleNameChange} />
           < button className = {'Next'} onClick={this.getPrevSubmit}> Previous Submission</button>
           < button className = {'Next'} onClick={this.getNextSubmit}> Next Submission</button>
           < button className = {'Next'} onClick={this.skipTweet}> Skip Tweet</button>
         </label>
         
       </fieldset>
       <fieldset>
       <img alt="twitter " src={this.state.picLoc}></img>
       <p>{this.state.twitterText}</p>
       <PlaceSurvey onFinish={this.onCompleteComponent}/>
      </fieldset>
    </div>
    )
  }
  if(this.state.exercise ==='Child' & this.state.enteredName===true) {
    return(
      <div>
      <h2>ASPIRE Social Media Labeling (version Sep 15th, 2022)</h2>
      <fieldset>
         <label>
           <p className='filler'>Please enter your user id in the text box below </p>
           <input name="name"  type="text"value = {this.state.userName} onChange={this.handleNameChange} />
           < button className = {'Next'} onClick={this.getPrevSubmit}> Previous Submission</button>
           < button className = {'Next'} onClick={this.getNextSubmit}> Next Submission</button>
           < button className = {'Next'} onClick={this.skipTweet}> Skip Tweet</button>
         </label>
         
       </fieldset>
       <fieldset>
       <img alt="twitter " src={this.state.picLoc}></img>
       <p>{this.state.twitterText}</p>
       <ChildSurvey onFinish={this.onCompleteComponent}/>
      </fieldset>
    </div>
    )
  }
  if(this.state.exercise ==='Health' & this.state.enteredName===true) {
    return(
      <div>
      <h2>ASPIRE Social Media Labeling (version Sep 15th, 2022)</h2>
      <fieldset>
         <label>
           <p className='filler'>Please enter your user id in the text box below </p>
           <input name="name"  type="text"value = {this.state.userName} onChange={this.handleNameChange} />
           < button className = {'Next'} onClick={this.getPrevSubmit}> Previous Submission</button>
           < button className = {'Next'} onClick={this.getNextSubmit}> Next Submission</button>
           < button className = {'Next'} onClick={this.skipTweet}> Skip Tweet</button>
         </label>
         
       </fieldset>
       <fieldset>
       <img alt="twitter " src={this.state.picLoc}></img>
       <p>{this.state.twitterText}</p>
       <HealthSurvey onFinish={this.onCompleteComponent}/>
      </fieldset>
    </div>
    )
  }

  if(this.state.enteredName===false) {
    return(
      <div>
        <h2>ASPIRE Social Media Labeling (version Sep 15th, 2022)</h2>
        <fieldset>
           <label>
           <p> <b>Warning:</b> Twitter records can contain text and/or imagery that may be offensive to some viewers.  Offensive content may include foul and agressive language, violence, hate speech, drug use, nuditity, and/or pornography.  By clicking on the I agree button below, you agree that you have been informed of potentially offensive material and are voluntarily choosing to view tweets</p>

             <p> Are you here for the training exercise, to practice or to code tweets?</p>
             <div onChange={this.onChangeExercise}>

               <input type="radio" value="Practice" name="Exercise"/> Practice <br></br>
               <input type="radio" value="Place" name="Exercise"/> Place <br></br>
               <input type="radio" value="Child" name="Exercise"/> Child <br></br>
               <input type="radio" value="Health" name="Exercise"/> Health <br></br>
             </div>

             <p className='filler'>Please enter your user id in the text box below  </p>
             <input name="name" type="text"value = {this.state.userName} onChange={this.handleNameChange} />
            

            
             < button className = {'Next'} onClick={this.enteredName}> I Agree</button>
             
           </label>
           
         </fieldset>
         </div>
)
  }
  return(
    <div>
      <h2>ASPIRE Social Media Labeling (version Sep 15th, 2022)</h2>
      <fieldset>
         <label>
           <p className='filler'>Please enter your user id in the text box below </p>
           <input name="name"  type="text"value = {this.state.userName} onChange={this.handleNameChange} />
           < button className = {'Next'} onClick={this.getPrevSubmit}> Previous Submission</button>
           < button className = {'Next'} onClick={this.getNextSubmit}> Next Submission</button>
           < button className = {'Next'} onClick={this.skipTweet}> Skip Tweet</button>
         </label>
         
       </fieldset>
       <fieldset>
       <img alt="twitter " src={this.state.picLoc}></img>
       <p>{this.state.twitterText}</p>
       <SurveyComponent onFinish={this.onCompleteComponent}/>    
      </fieldset>
      
    </div>
  )
}


async onSearchSubmit(sampleNum,specific=false) {
  let httpCoding = 'insertbackendwebsitehttp/sample?id=' + sampleNum.toString();
  if(specific == true) {
    httpCoding = 'insertbackendwebsitehttp/sample?id=' + sampleNum.toString();
  }
  else if(this.state.exercise=="Place") {
    httpCoding = 'insertbackendwebsitehttp/place?user=' + this.state.userName;
  }
  else if(this.state.exercise=="Child") {
    httpCoding = 'insertbackendwebsitehttp/child?user=' + this.state.userName;
  }
  else if(this.state.exercise=="Health") {
    httpCoding = 'insertbackendwebsitehttp/health?user=' + this.state.userName;
  }

  const response = await axios.get(httpCoding,
  {
      headers: {
          'Access-Control-Allow-Origin': '*',
      },
      proxy: {
          host: '127.0.0.1',
          port: 17995
      }
  }
  );
  

  let curQueue = this.state.prevSubmit;
  let curIndex = this.state.queueIndex;
  if(curIndex===curQueue.tail-1) {
    if(curQueue.getLength() >=5) {
      curQueue.dequeue()
      curIndex+=1
    }
      curQueue.enqueue(response.data.imgId)
  }
  if(curIndex<curQueue.tail-1) {
      curIndex+=1;
  }
  this.setState(
    {
        twitterId: parseInt(response.data.imgId),
        twitterText: response.data.text,
        picLoc: "insertAmazonBucketHttp/" + response.data.imgHttp,
        prevSubmit:curQueue,
        queueIndex:curIndex,
        qa:response.data.qa
    }
);

};


onCompleteComponent = (payloadData) =>  {

  let dataToSend = {
    'imgId':this.state.twitterId,
    'userId':this.state.userName,
    'location':[""],
    'isChild':false,
    'age':[""],
    'isHealth':false,
    'healthImpact':[""],
    'healthType':[""],
    'childInPic':false,
    'locationCat':[""],
    'qa':this.state.qa
  }
  let keys = Object.keys(payloadData);
  for(let i=0; i< keys.length; i++) {
    dataToSend[keys[i]] = payloadData[keys[i]];
  }
  this.sendResults(dataToSend);
  
  if(this.state.queueIndex===this.state.prevSubmit.tail-1){

    this.onSearchSubmit(this.state.twitterId+1);
  }
  else{
    let searchId = this.state.prevSubmit.peek(this.state.queueIndex+1)
    this.onSearchSubmit(searchId);
  }
}

async sendResults(resultsToSend) {
  console.log("sending results");
  const response = await axios.post
  (
    'insertbackendwebsitehttp/submit',
      resultsToSend
  )
}


}
export default App;
