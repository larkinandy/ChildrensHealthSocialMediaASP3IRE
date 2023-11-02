
import React from 'react';
import "./styles.css";
import * as Survey from "survey-react";
import "survey-react/survey.css";

class SurveyComponent extends React.Component {
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
        },
        {
        "type": "boolean",
            "name": "isChild",
            "title": "Please answer the question",
            "label": "Does the tweet mention or depict a child or children?",
            "isRequired": true
        },
        {
          type: "checkbox",
          name: "age",
          title: "What age group does the child(ren) belong to (can select multiple)?",
          isRequired: false,
          hasSelectAll: false,
          hasNone: false,
          noneText: "None of the above",
          colCount: 3,
          visibleIf: "{isChild}==true",
          choices: [
           "0 to less than 1 year (baby/infant)",
           "1 to 4 years (toddler/pre-school)",
           "5 to 10 years (elementary school)",
           "11 to 13 years (middle school)",
           "14 to 17 years (high school)",
           "School age (no specific school type)",
           "No specific age (children general)",
           "Unsure"
          ]
        },
        {
          type: "boolean",
          name: "isHealth",
          title: "Is a health symptom or outcome mentioned in the tweet (doesn't have to be a child)?",
          isRequired: true,
        },
        { 
          type: "checkbox",
          name: "healthImpact",
          title: "Does the tweet author imply a negative or positive health impact (can select multiple)?",
          isRequired:false,
          hasSelectAll:false,
          hasNone: false,
          colCount: 3,
          visibleIf: "{isHealth}==true",
          choices: [
            "Negative impact",
            "Positive impact",
            "No impact"
          ]
        },
        {
          type: "checkbox",
          name: "healthType",
          title: "Is (are) the health symptom(s) or outcome(s) referring to cognitive, emotional/social, or physical health (can select multiple)?",
          isRequired:false,
          hasSelectAll:false,
          hasNone: false,
          colCount: 3,
          visibleIf: "{isHealth}==true",
          choices: [
            "Cognitive health",
            'Emotional/social health',
            "Physical health"
          ]
        },
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

  
export default SurveyComponent;