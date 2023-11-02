
import React from 'react';
import "./styles.css";
import * as Survey from "survey-react";
import "survey-react/survey.css";

class PlaceSurvey extends React.Component {
    constructor(props) {
      super(props);
      this.state = { 
        isCompleted:false
       };
      this.onCompleteComponent = this.onCompleteComponent.bind(this);
    }



    onCompleteComponent = (props) =>  {
      this.props.onFinish(props.data);
      this.model.clear();
    }


    render() {
      var defaultQuestions =  [
        
        {
        type: "checkbox",
        name: "location",
        title: "Can you identify a location form the text, hashtags, and/or image? (can select multiple)?",
        isRequired:false,
        hasSelectAll:false,
        hasNone:false,
        colCount: 3,
        //visibleIf: '{locationCat} contains "Indoor location" || {locationCat} contains "Outdoor location"',
        choices: [
          "Childcare/daycare",
          "A home",
          "Neigborhood (but not on home property, etc)",
          "Park/playground/child sports center",
          "School",
          "Other",
          "Unsure",
          "No location"
        ]
        },{
          type: "checkbox",
          name: "locationCat",
          title: "Can you tell if the location is indoors or outdoors? (can select multiple)?",
          isRequired: true,
          hasSelectAll: false,
          hasNone:false,
          //visibleIf: '({location} contains "Unsure (try to guess, only use if really unsure)")==False ',
          colCount: 3,
          choices: [
            "Indoor location",
            "Outdoor location",
            "No location/unsure"
          ]
        }
      ]
     
      this.model = new Survey.Model({questions: defaultQuestions});

      var surveyRender = (
        <Survey.Survey
         model={this.model}
          showCompletedPage={false}
          onComplete={this.onCompleteComponent}
        />
      );
  
      return (
        <div>
          {surveyRender} 
        </div>
      );
    }
  }

  
export default PlaceSurvey;