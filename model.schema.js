import mongoose from "mongoose";

const DLQSchema = new mongoose.Schema({
    to:{
        type: String,
        required: true
    },
    subject:{
        type: String,
        required: true
    },
    text:{
        type: String,
        required: true
    },
    html:{
        type: String,
        required: true
    },
    reason:{
        type: String
    }
    
}, { timestamps: true })

const DLQModel = mongoose.model("DLQ", DLQSchema);

export default DLQModel;
