
import React from 'react';
import "./styles.css";
import * as Survey from "survey-react";
import "survey-react/survey.css";

class HealthSurvey extends React.Component {
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

  
export default HealthSurvey;