import {ScorePreset} from "../core/models/score-preset.model";

export const SCORE_PRESETS: ScorePreset[] = [
    {
        id: 'standard',
        name: 'Standard',

        values: {
            'plants': 1,
            'monthly-card': 6,
            'ribbons': 1,
            'poetry-ribbons': 6,
            'blue-ribbons': 6,
            'animals': 1,
            'boar-deer-butterfly': 6,
            'three-brights': 6,
            'four-brights': 10,
            'four-rainy-brights': 8,
            'five-brights': 15,
            'moon-viewing-sake': 5,
            'flower-viewing-sake': 5
        }
    },
    {
        id: 'iello',
        name: 'IELLO',

        // TODO: Change values
        values: {
            'plants': 1,
            'monthly-card': 6,
            'ribbons': 1,
            'poetry-ribbons': 6,
            'blue-ribbons': 6,
            'animals': 1,
            'boar-deer-butterfly': 6,
            'three-brights': 6,
            'four-brights': 10,
            'four-rainy-brights': 8,
            'five-brights': 15,
            'moon-viewing-sake': 5,
            'flower-viewing-sake': 5
        }
    }
]